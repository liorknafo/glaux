-- Trino packs a timestamp with time zone into 52 bits of milliseconds; an infinite epoch overflows it.
-- error: Millis overflow
SELECT from_unixtime(infinity())
