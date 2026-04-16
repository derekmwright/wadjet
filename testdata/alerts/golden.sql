-- Minimal webhook alert.
CREATE ALERT minimal_webhook AS
  SELECT 1 AS n FROM t
EVERY 10 SECONDS
WEBHOOK 'http://example.com/hook';

-- Full DDL: webhook with headers + history table.
CREATE ALERT full_alert AS
  SELECT user_id, COUNT(*) AS c FROM events GROUP BY user_id HAVING COUNT(*) > 10
EVERY 5 MINUTES
WEBHOOK 'https://api.example.com/v2/alerts'
  HEADERS { 'Authorization' = 'Token abc123', 'X-Env' = 'prod' }
INSERT INTO alert_history;

-- INSERT-only alert (no webhook).
CREATE ALERT log_only AS
  SELECT * FROM errors WHERE severity = 'fatal'
EVERY 1 HOURS
INSERT INTO alert_history;

-- Lifecycle.
ALTER ALERT minimal_webhook DISABLE;
ALTER ALERT minimal_webhook ENABLE;
DROP ALERT IF EXISTS log_only;
DROP ALERT minimal_webhook;
