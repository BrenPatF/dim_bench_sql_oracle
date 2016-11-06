BREAK ON id
COLUMN group_text FORMAT A20
COLUMN text FORMAT A200
COLUMN "Time" FORMAT A8
SET LINES 230
SET PAGES 10000
SELECT /*group_text, */line_text text
  FROM log_lines
 WHERE log_header_id = 0
 ORDER BY id
/
