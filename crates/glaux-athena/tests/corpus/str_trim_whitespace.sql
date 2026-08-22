-- trim / ltrim / rtrim strip every Java whitespace code point (not NBSP); TRIM syntax strips a character set.
SELECT length(trim(chr(9) || 'a' || chr(10))) AS tab_lf,
       length(ltrim(chr(9) || 'a')) AS ltrim_tab,
       length(rtrim('a' || chr(13))) AS rtrim_cr,
       length(trim(chr(11) || chr(12) || 'a' || chr(28) || chr(31))) AS vt_ff_fs_us,
       length(trim(chr(8232) || 'a' || chr(12288))) AS line_sep_ideographic,
       length(trim(chr(160) || 'a' || chr(8239))) AS nbsp_kept,
       trim(note) AS trimmed_note,
       TRIM(LEADING 'x' FROM 'xax') AS leading_x,
       TRIM(TRAILING 'xy' FROM 'axyyx') AS trailing_set,
       TRIM(BOTH 'y' FROM 'yay') AS both_y,
       TRIM('z' FROM 'zaz') AS default_both,
       ltrim('  a  ') || '|' AS ltrim_spaces,
       rtrim('  a  ') || '|' AS rtrim_spaces
FROM orders WHERE id = 102
