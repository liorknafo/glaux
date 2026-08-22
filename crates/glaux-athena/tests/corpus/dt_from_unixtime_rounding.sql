-- from_unixtime rounds to the millisecond with Java's Math.round: half towards positive infinity, so the negative ties round up (Math.round(-0.5) = 0), and NaN rounds to the epoch.
SELECT CAST(from_unixtime(-0.0005) AS VARCHAR) AS neg_half_ms,
       CAST(from_unixtime(-0.0015) AS VARCHAR) AS neg_one_and_a_half_ms,
       CAST(from_unixtime(-1.0005) AS VARCHAR) AS neg_second_and_half_ms,
       CAST(from_unixtime(0.0005) AS VARCHAR) AS pos_half_ms,
       CAST(from_unixtime(0.0015) AS VARCHAR) AS pos_one_and_a_half_ms,
       CAST(from_unixtime(1.9999) AS VARCHAR) AS rounded_up,
       CAST(from_unixtime(-0.0004) AS VARCHAR) AS below_half_ms,
       CAST(from_unixtime(nan()) AS VARCHAR) AS not_a_number,
       CAST(from_unixtime(0) AS VARCHAR) AS epoch,
       CAST(from_unixtime(-1) AS VARCHAR) AS before_epoch
