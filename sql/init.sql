CREATE TABLE IF NOT EXISTS base_station_quality (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  country VARCHAR(50) NOT NULL,
  operator_name VARCHAR(100) NOT NULL,
  station_id VARCHAR(100) NOT NULL,
  event_date DATE NOT NULL,
  failure_count INT NOT NULL DEFAULT 0,
  total_count INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_quality_country_date
ON base_station_quality(country, event_date);
