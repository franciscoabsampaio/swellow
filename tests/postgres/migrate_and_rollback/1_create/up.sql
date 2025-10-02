CREATE TABLE flock (
    bird_id SERIAL PRIMARY KEY,
    common_name TEXT NOT NULL,
    latin_name TEXT NOT NULL,
    wingspan_cm INTEGER,
    dtm_hatched_at TIMESTAMP DEFAULT now(),
    dtm_last_seen_at TIMESTAMP DEFAULT now()
);