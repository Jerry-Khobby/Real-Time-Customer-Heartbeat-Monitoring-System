-- This query counts how many heartbeats fall under each status
-- (e.g., normal, tachycardia, bradycardia)
-- within the selected Grafana time range.

SELECT status, COUNT(*) AS count
FROM heartbeats
WHERE $__timeFilter(timestamp)  
GROUP BY status;                



-- This query returns individual heart rate readings over time.
-- Used for time-series visualization in Grafana.

SELECT 
    timestamp AS time,              
    heart_rate,                    
    CAST(patient_id AS TEXT) AS metric 
FROM heartbeats
WHERE $__timeFilter(timestamp)
ORDER BY timestamp;




-- This query calculates the percentage of abnormal heartbeats
-- (tachycardia + bradycardia) within the selected time range.

SELECT
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE status != 'normal') 
    / COUNT(*), 
    2
  ) AS anomaly_rate_pct
FROM heartbeats
WHERE $__timeFilter(timestamp);


-- This query calculates the average heart rate per minute.
-- Useful for smoothing short-term fluctuations.

SELECT
  date_trunc('minute', timestamp) AS time, 
  ROUND(AVG(heart_rate), 1) AS avg_heart_rate
FROM heartbeats
WHERE $__timeFilter(timestamp)
GROUP BY 1               
ORDER BY 1;              


-- This query retrieves the most recent heartbeat record for each patient.
-- Used for displaying "live" patient status.

SELECT DISTINCT ON (patient_id)
  patient_id,
  heart_rate,
  status,
  timestamp
FROM heartbeats
ORDER BY patient_id, timestamp DESC;



-- This query counts abnormal heartbeats per minute.
-- Helps visualize when spikes in anomalies occur.

SELECT
  date_trunc('minute', timestamp) AS time,
  COUNT(*) AS anomalies
FROM heartbeats
WHERE status != 'normal'
  AND $__timeFilter(timestamp)
GROUP BY 1
ORDER BY 1;