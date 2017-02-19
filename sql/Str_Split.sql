SET LINES 130
SET PAGES 1000
SET TRIMSPOOL ON
SPOOL ..\out\Str_Split
EXEC Utils.Clear_Log;
DROP TABLE delimited_lists PURGE
/
CREATE TABLE delimited_lists(id INT, list_col VARCHAR2(4000))
/
INSERT INTO delimited_lists VALUES(1,'token_11|token_12')
/
INSERT INTO delimited_lists VALUES(2,'|token_21|token_22')
/
INSERT INTO delimited_lists VALUES(3,'token_31|token_32|')
/
INSERT INTO delimited_lists VALUES(4,'token_41||token_42')
/
INSERT INTO delimited_lists VALUES(5,'|')
/
COMMIT;

COLUMN id FORMAT 999990
COLUMN token FORMAT A10
COLUMN list_col FORMAT A60

PROMPT Test Data
SELECT * FROM delimited_lists
/
SET TIMING ON
PROMPT xmltable

SELECT /*+ XML_QRY gather_plan_statistics */ id, x2 token
  FROM delimited_lists, XMLTable (
    'if (contains($X2,"|")) then ora:tokenize($X2,"\|") else $X2'
  PASSING list_col AS x2
  COLUMNS x2 VARCHAR2(4000) PATH '.'
)
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'XML_QRY');

PROMPT MULTISET

SELECT /*+ MUL_QRY gather_plan_statistics */
       d.id, Substr (d.list_col, Instr ('|' || d.list_col, '|', 1, t.COLUMN_VALUE),
                        Instr (d.list_col || '|', '|', 1, t.COLUMN_VALUE) -
                        Instr ('|' || d.list_col, '|', 1, t.COLUMN_VALUE)) token
  FROM delimited_lists d, 
       TABLE (CAST (MULTISET (SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= Nvl (Length(d.list_col), 0) - Nvl (Length (Replace (d.list_col, '|')), 0) + 1) AS SYS.ODCINumberlist)) t
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'MUL_QRY');

PROMPT sys_guid

WITH guid_cby AS (
  SELECT id, level rn, list_col,Instr ('|' || d.list_col, '|', 1, LEVEL) pos
    FROM delimited_lists d
  CONNECT BY prior id = id and prior sys_guid() is not null and
    LEVEL <= Length (d.list_col) - Nvl (Length (Replace (d.list_col, '|')), 0) + 1
)
SELECT /*+ GUI_QRY gather_plan_statistics */
    id, 
    Substr (list_col, pos, Lead (pos, 1, 4000) OVER (partition by id ORDER BY pos) - pos - 1) token
  FROM guid_cby
 ORDER BY 1, 2
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'GUI_QRY');

PROMPT Model

SELECT /*+ MOD_QRY gather_plan_statistics */
       id,
       token
  FROM delimited_lists
 MODEL
    PARTITION BY (id)
    DIMENSION BY (1 rn)
    MEASURES (CAST('' AS VARCHAR2(4000)) AS token, '|' || list_col || '|' list_col, 2 pos, 0 nxtpos, Length(list_col) + 2 len)
    RULES ITERATE (2000) UNTIL pos[1] > len[1] (
       nxtpos[1] = Instr (list_col[1], '|', pos[1], 1),
       token[iteration_number+1] = Substr (list_col[1], pos[1], nxtpos[1] - pos[1]),
       pos[1] = nxtpos[1] + 1
    )
 ORDER BY 1, 2
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'MOD_QRY');

PROMPT Recursive subquery factor

WITH rsf (id, token, nxtpos, nxtpos2, list_col, len, iter) AS
(
SELECT id,
       Substr (list_col, 1, Instr (list_col || '|', '|', 1, 1) - 1),
       Instr (list_col || '|', '|', 1, 1) + 1,
       Instr (list_col || '|', '|', 1, 2),
       list_col || '|',
       Length (list_col) + 1,
       1
  FROM delimited_lists
UNION ALL
SELECT id,
       Substr (list_col, nxtpos, nxtpos2 - nxtpos), --Instr (list_col, '|', 1, iter + 1) - nxtpos),
       nxtpos2 + 1, --Instr (list_col, '|', 1, iter + 1) + 1,
       Instr (list_col, '|', nxtpos2 + 1, 1),
       list_col,
       len,
       iter + 1
  FROM rsf r
 WHERE nxtpos <= len
)
SELECT /*+ RSF_QRY gather_plan_statistics */
       id,
       token
  FROM rsf
 ORDER BY 1, 2
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'RSF_QRY');

PROMPT Row generator, Instr 2000

WITH row_gen AS (
        SELECT LEVEL rn FROM DUAL CONNECT BY LEVEL < 2000
)
SELECT /*+ RGN_QRY gather_plan_statistics leading (d) */
       d.id, Substr (d.list_col, Instr ('|' || d.list_col, '|', 1, r.rn),
                        Instr (d.list_col || '|', '|', 1, r.rn) -
                        Instr ('|' || d.list_col, '|', 1, r.rn)) token
  FROM delimited_lists d
  JOIN row_gen r
    ON r.rn <= Nvl (Length(d.list_col), 0) - Nvl (Length (Replace (d.list_col,'|')), 0) + 1
 ORDER BY 1, 2
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'RGN_QRY');
PROMPT Row generator, Instr - compute

WITH row_gen AS (
        SELECT LEVEL rn FROM DUAL CONNECT BY LEVEL <= 
            (SELECT Max (Nvl (Length(d.list_col), 0) - Nvl (Length (Replace (d.list_col,'|')), 0) + 1)
               FROM delimited_lists d)
)
SELECT /*+ RGM_QRY gather_plan_statistics leading (d) */
       d.id, Substr (d.list_col, Instr ('|' || d.list_col, '|', 1, r.rn),
                        Instr (d.list_col || '|', '|', 1, r.rn) -
                        Instr ('|' || d.list_col, '|', 1, r.rn)) token
  FROM delimited_lists d
  JOIN row_gen r
    ON r.rn <= Nvl (Length(d.list_col), 0) - Nvl (Length (Replace (d.list_col,'|')), 0) + 1
 ORDER BY 1, 2
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'RGM_QRY');

PROMPT Row generator, MR

WITH row_gen AS (
        SELECT LEVEL rn FROM DUAL CONNECT BY LEVEL <= 4000
), char_streams AS (
SELECT d.id, r.rn, Substr (d.list_col || '|', r.rn, 1) chr
  FROM delimited_lists d
  JOIN row_gen r
    ON r.rn <= Nvl (Length(d.list_col), 0) + 2
), chars_grouped AS (
SELECT *
  FROM char_streams
 MATCH_RECOGNIZE (
   PARTITION BY id
   ORDER BY rn
   MEASURES chr mchr,
            FINAL COUNT(*) n_chrs,
            MATCH_NUMBER() mno
      ALL ROWS PER MATCH
  PATTERN (c*? d)
   DEFINE d AS d.chr = '|'
  ) m
)
SELECT /*+ RMR_QRY gather_plan_statistics */
       id, 
      /* RTrim (Listagg (chr, '') WITHIN GROUP (ORDER BY rn), '|') TOKEN */
       Listagg (chr, '') WITHIN GROUP (ORDER BY rn) token
  FROM chars_grouped
GROUP BY id, mno
ORDER BY 1, 2
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'RMR_QRY');

PROMPT Row generator, regex

WITH row_gen AS (
        SELECT LEVEL rn FROM DUAL CONNECT BY LEVEL < 2000
)
SELECT /*+ RGX_QRY gather_plan_statistics leading (d) */
       d.id, RTrim (Regexp_Substr (d.list_col || '|', '(.*?)\|', 1, r.rn), '|') token
  FROM delimited_lists d
  JOIN row_gen r
    ON r.rn <= Nvl (Length(d.list_col), 0) - Nvl (Length (Replace (d.list_col,'|')), 0) + 1
ORDER BY d.id, r.rn
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'RGX_QRY');

PROMPT Pipelined function

SELECT /*+ PLF_QRY gather_plan_statistics */
       d.id, s.COLUMN_VALUE token
  FROM delimited_lists d
 CROSS JOIN TABLE (Strings.Split(d.list_col, '|')) s
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'PLF_QRY');
PROMPT WITH function

WITH FUNCTION Split (p_string VARCHAR2, p_delim VARCHAR2) RETURN L1_chr_db_arr IS
  c_delim_len   CONSTANT SIMPLE_INTEGER := Length(p_delim);
  l_token_start          SIMPLE_INTEGER := 1;
  l_next_delim           SIMPLE_INTEGER := Instr (p_string, p_delim, l_token_start, 1);
  l_ret_arr              L1_chr_db_arr := L1_chr_db_arr();

BEGIN

  WHILE l_next_delim > 0 LOOP
    l_ret_arr.EXTEND;
    l_ret_arr(l_ret_arr.COUNT) := Substr (p_string, l_token_start, l_next_delim - l_token_start);
    l_token_start := l_next_delim + c_delim_len;
    l_next_delim := Instr (p_string || p_delim, p_delim, l_token_start, 1);
  END LOOP;
  RETURN l_ret_arr;

END Split;
SELECT /*+ WFN_QRY gather_plan_statistics */
       d.id, s.COLUMN_VALUE token
  FROM delimited_lists d
 CROSS JOIN TABLE (Split(d.list_col, '|')) s
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'WFN_QRY');

SELECT  /*+ LAT_QRY gather_plan_statistics */
    d.id                id,
    l.subs              token
FROM delimited_lists d,
LATERAL (
  SELECT Substr (d.list_col, pos + 1, Lead (pos, 1, 4000) OVER (ORDER BY pos) - pos - 1) subs, pos
    FROM (
    SELECT Instr (d.list_col, '|', 1, LEVEL) pos
      FROM DUAL
    CONNECT BY
      LEVEL <= Length (d.list_col) - Nvl (Length (Replace (d.list_col, '|')), 0) + 1
    )
) l
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'LAT_QRY');

PROMPT Regex

WITH row_gen AS (
        SELECT LEVEL rn FROM DUAL CONNECT BY LEVEL < 2000
)
SELECT /*+ RGX_QRY gather_plan_statistics */
    d.id   id,
    Regexp_Replace (Regexp_Substr (d.list_col || '|', '(.*?)\|', 1, r.rn), '\|$', '') token
  FROM delimited_lists d
  JOIN row_gen r
    ON r.rn <= Nvl (Length(d.list_col), 0) - Nvl (Length (Replace (d.list_col,'|')), 0) + 1
ORDER BY 1, 2
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'RGX_QRY');

SET TIMING OFF
@..\sql\L_Log_Default
SPOOL OFF

