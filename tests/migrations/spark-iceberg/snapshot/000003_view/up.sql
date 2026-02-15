CREATE VIEW bird_watch.flock_summary AS
SELECT common_name, COUNT(*) AS count
FROM bird_watch.flock
GROUP BY common_name;