# Wadjet CREATE ALERT

Define a SQL-driven alert that evaluates a query periodically and delivers matching rows to a webhook, a history table, or both.

## Grammar

    CREATE ALERT <name>
      AS <SELECT ...>
      EVERY <N> {SECONDS|MINUTES|HOURS}
      [ WEBHOOK '<url>' [HEADERS { 'K' = 'V', ... }] ]
      [ INSERT INTO <table> ]
      ;

    DROP ALERT [IF EXISTS] <name> ;
    ALTER ALERT <name> {ENABLE|DISABLE} ;

At least one of `WEBHOOK` or `INSERT INTO` is required. Minimum interval is 10 seconds.

## Example

    CREATE ALERT failed_logins_spike AS
      SELECT user_id, COUNT(*) AS failures
      FROM auth_events
      WHERE event_type = 'login_failed' AND ts >= now() - INTERVAL '5 minutes'
      GROUP BY user_id HAVING COUNT(*) > 10
    EVERY 5 MINUTES
    WEBHOOK 'https://example/alert'
    INSERT INTO alert_history;

## Semantics

- **Stateless polling.** The query re-runs every interval; a persistent condition will re-fire every tick.
- **One fire per evaluation.** The first 1000 matching rows are sent in the payload; `truncated=true` when `row_count > 1000`.
- **Leader-only execution.** Exactly one coordinator runs the scheduler at a time.
- **No delivery if zero rows match.**

## Limits

- Interval floor: 10 seconds.
- Row payload cap: 1000 rows per fire.
- Max alerts per cluster (soft): ~100.
