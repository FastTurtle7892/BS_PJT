from fastapi import FastAPI, UploadFile, File
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import Optional
import pandas as pd
import re

app = FastAPI()

engine = create_engine(
    "mysql+pymysql://app:apppw@localhost:3307/bs_platform",
    pool_pre_ping=True,
)


class NLQueryRequest(BaseModel):
    question: str


def _extract_country(question: str) -> Optional[str]:
    q = question.lower()
    if "한국" in q or "kr" in q:
        return "KR"
    if "미국" in q or "us" in q:
        return "US"

    # fallback: two-letter country code in text
    m = re.search(r"\b([A-Za-z]{2})\b", question)
    return m.group(1).upper() if m else None


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/ingest/csv")
async def ingest_csv(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)

    required = [
        "country",
        "operator_name",
        "station_id",
        "event_date",
        "failure_count",
        "total_count",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        return {"ok": False, "missing_columns": missing}

    df = df[required].copy()
    df["failure_count"] = df["failure_count"].fillna(0).astype(int)
    df["total_count"] = df["total_count"].fillna(0).astype(int)

    rows = df.to_dict(orient="records")

    insert_sql = text(
        """
        INSERT INTO base_station_quality
        (country, operator_name, station_id, event_date, failure_count, total_count)
        VALUES
        (:country, :operator_name, :station_id, :event_date, :failure_count, :total_count)
        """
    )

    with engine.begin() as conn:
        conn.execute(insert_sql, rows)

    return {"ok": True, "inserted": len(rows)}


@app.get("/kpi/failure-rate")
def failure_rate(country: str):
    query_sql = text(
        """
        SELECT
          country,
          SUM(failure_count) AS failure_sum,
          SUM(total_count) AS total_sum,
          CASE
            WHEN SUM(total_count) = 0 THEN 0
            ELSE ROUND(SUM(failure_count) / SUM(total_count), 4)
          END AS failure_rate
        FROM base_station_quality
        WHERE country = :country
        GROUP BY country
        """
    )

    with engine.begin() as conn:
        row = conn.execute(query_sql, {"country": country}).mappings().first()

    return {"ok": True, "data": dict(row) if row else None}


@app.post("/query/nl")
def query_nl(req: NLQueryRequest):
    question = req.question.strip()
    country = _extract_country(question)
    if not country:
        return {"ok": False, "error": "국가를 질문에 포함해줘. 예: KR 불량률 알려줘"}

    q = question.lower()

    # 1) failure rate
    if "불량률" in q or "failure rate" in q:
        sql = text(
            """
            SELECT
              country,
              SUM(failure_count) AS failure_sum,
              SUM(total_count) AS total_sum,
              CASE
                WHEN SUM(total_count) = 0 THEN 0
                ELSE ROUND(SUM(failure_count) / SUM(total_count), 4)
              END AS failure_rate
            FROM base_station_quality
            WHERE country = :country
            GROUP BY country
            """
        )

    # 2) failure count
    elif "불량" in q or "failure count" in q:
        sql = text(
            """
            SELECT country, SUM(failure_count) AS failure_sum
            FROM base_station_quality
            WHERE country = :country
            GROUP BY country
            """
        )

    # 3) total count
    elif "총 건수" in q or "total count" in q:
        sql = text(
            """
            SELECT country, SUM(total_count) AS total_sum
            FROM base_station_quality
            WHERE country = :country
            GROUP BY country
            """
        )
    else:
        return {"ok": False, "error": "지원 질문: 불량률, 불량 수, 총 건수"}

    with engine.begin() as conn:
        row = conn.execute(sql, {"country": country}).mappings().first()

    return {
        "ok": True,
        "question": question,
        "data": dict(row) if row else None,
    }


@app.get("/anomaly/simple")
def anomaly_simple(threshold: float = 0.03):
    sql = text(
        """
        SELECT
          country,
          SUM(failure_count) AS failure_sum,
          SUM(total_count) AS total_sum,
          CASE
            WHEN SUM(total_count) = 0 THEN 0
            ELSE ROUND(SUM(failure_count) / SUM(total_count), 4)
          END AS failure_rate
        FROM base_station_quality
        GROUP BY country
        HAVING failure_rate >= :threshold
        ORDER BY failure_rate DESC
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql, {"threshold": threshold}).mappings().all()

    return {"ok": True, "threshold": threshold, "items": [dict(r) for r in rows]}
