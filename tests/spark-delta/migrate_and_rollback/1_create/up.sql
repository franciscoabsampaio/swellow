CREATE TABLE flock (
    bird_id BIGINT,
    common_name STRING NOT NULL,
    latin_name STRING NOT NULL,
    wingspan_cm INT,
    dtm_hatched_at TIMESTAMP,
    dtm_last_seen_at TIMESTAMP
)
USING delta
LOCATION '/opt/spark/spark-warehouse/flock';