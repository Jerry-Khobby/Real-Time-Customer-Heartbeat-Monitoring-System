CREATE TABLE IF NOT EXISTS heartbeats(
  id SERIAL PRIMARY KEY, 
  patient_id INT NOT NULL, 
  timestamp TIMESTAMP NOT NULL, 
  heart_rate  INT NOT NULL, 
  status  VARCHAR(20)
)