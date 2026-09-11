# Time bucket stride origin design

Source: internal/engine/expr/time_bucket.go — func fnTimeBucket(args []any) any {, moved 2026-09-11 (#1026)

TIME_BUCKET — PostgreSQL's `date_bin`, under the name every time-series
engine spells it.

	time_bucket(stride, source)          -- origin defaults to 1970-01-01
	time_bucket(stride, source, origin)

It answers the largest multiple of `stride` measured from `origin` that does
not exceed `source`, which is what makes it the GROUP BY key of a
downsampled bar: every row of one bucket maps to one instant, and that
instant is a real TIMESTAMP (OID 1114) rather than text.

PostgreSQL 17 is the oracle for every one of its answers, measured live on
2026-09-08:

	date_bin('15 min','2020-02-11 15:44:17','2001-01-01') -> 2020-02-11 15:30:00
	date_bin('15 min','2020-02-11 15:45:00','2001-01-01') -> 2020-02-11 15:45:00
	date_bin('1 hour','1969-07-20 20:17:40','1970-01-01') -> 1969-07-20 20:00:00
	date_bin('1 day','1969-12-30 23:59:59.999999',epoch)  -> 1969-12-30 00:00:00
	date_bin('1 hour', ts, '2030-01-01 00:30:00')         -> bins aligned at :30
	any NULL argument                                     -> NULL
	a stride containing MONTHS or YEARS                   -> 0A000
	a stride of zero or less                              -> 22008

Two properties of that list are load-bearing and easy to get wrong:

  - The division FLOORS toward negative infinity, not toward zero. Before
    1970 a truncating division would answer the bucket ABOVE the row, so
    every pre-epoch row would be filed under a bucket it does not belong to
    and the bar for that bucket would mix two of them.
  - The bucket boundary belongs to the bucket it OPENS. `15:45:00` with a
    15-minute stride is `15:45:00`, not `15:30:00`.

The stride is a wall-clock span, so this engine's UTC instants make every
bucket exactly `stride` wide. There is no DST to make an hour bucket 3600 or
7200 seconds depending on the day, which is the ambiguity PostgreSQL itself
avoids by refusing months and years: those are the calendar units whose
length depends on WHERE they land, and neither engine will guess.

The stride must be spelled as an INTERVAL literal. That keeps the accepted
grammar exactly the one `plansql.parseIntervalLiteral` already defines —
a single `N unit` pair — rather than growing a second interval parser here
that would agree with the first only by inspection.
