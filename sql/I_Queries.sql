/***************************************************************************************************
Description: Bench_SQL SQL benchmarking framework - test queries across a 2-d dataset space

             Installation script for bursting problem queries

Further details: A Framework for Dimensional Benchmarking of SQL Performance 
                 http://aprogrammerwrites.eu/?p=1833

Modification History
Who                  When        Which What
-------------------- ----------- ----- -------------------------------------------------------------
Brendan Furey        05-Nov-2016 1.0   Created

***************************************************************************************************/
PROMPT BURST
DECLARE
  c_query_group         CONSTANT VARCHAR2(30) := 'BURST';
  c_group_description   CONSTANT VARCHAR2(30) := 'Bursting';
BEGIN

  Bench_Queries.Add_Query (p_query_name => 'MOD_QRY', p_description => 'Model clause', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
WITH all_rows AS (  
SELECT
      person_id,
      start_date,
      end_date,
      group_start
  FROM activity
 MODEL
    PARTITION BY (person_id)
    DIMENSION BY (Row_Number() OVER (PARTITION BY person_id ORDER BY start_date) rn)
    MEASURES (start_date, end_date, start_date group_start)
    RULES (
       group_start[rn = 1] = start_date[cv()],
       group_start[rn > 1] = CASE WHEN start_date[cv()] - group_start[cv()-1] > Sys_Context('bench_ctx', 'deep') THEN start_date[cv()] ELSE group_start[cv()-1] END
    )
)
SELECT 
/* SEL */
        person_id       person_id,
        group_start     group_start,
        MAX(end_date)   group_end,
        COUNT(*)        num_rows
/* SEL */
  FROM all_rows
GROUP BY person_id, group_start
ORDER BY person_id, group_start
!');

  Bench_Queries.Add_Query (p_query_name => 'RSF_QRY', p_description => 'Recursive subquery factoring', p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
WITH act AS (
SELECT person_id, start_date, end_date, Row_Number() OVER (PARTITION BY person_id ORDER BY start_date) rn
  FROM activity
),	rsq (person_id, rn, start_date, end_date, group_start) AS (
SELECT person_id, rn, start_date, end_date, start_date
  FROM act
 WHERE rn = 1
 UNION ALL
SELECT  act.person_id,
        act.rn,
        act.start_date,
        act.end_date,
        CASE WHEN act.start_date - rsq.group_start <= Sys_Context('bench_ctx', 'deep') THEN rsq.group_start ELSE act.start_date end
  FROM act
  JOIN rsq
    ON rsq.rn              = act.rn - 1
   AND rsq.person_id       = act.person_id
)
SELECT
/* SEL */
        person_id       person_id,
        group_start     group_start,
        Max (end_date)  group_end,
        COUNT(*)        num_rows
/* SEL */
FROM rsq
GROUP BY person_id, group_start
ORDER BY person_id, group_start
!');

  Bench_Queries.Add_Query (p_query_name => 'RSF_TMP', p_description => 'Recursive subquery factoring', p_active_yn => 'Y', p_query_group => c_query_group, 
        p_group_description => c_group_description, 
        p_pre_query_sql => 'INSERT INTO activity_tmp SELECT person_id, start_date, end_date, Row_Number() OVER (PARTITION BY person_id ORDER BY start_date) FROM activity',
        p_text =>
q'!
WITH rsq (person_id, rn, start_date, end_date, group_start) AS (
SELECT person_id, act_rownum, start_date, end_date, start_date
  FROM activity_tmp
 WHERE act_rownum = 1
 UNION ALL
SELECT  act.person_id,
        act.act_rownum,
        act.start_date,
        act.end_date,
        CASE WHEN act.start_date - rsq.group_start <= Sys_Context('bench_ctx', 'deep') THEN rsq.group_start ELSE act.start_date end
  FROM rsq
  JOIN activity_tmp act
    ON act.act_rownum     = rsq.rn + 1
   AND act.person_id      = rsq.person_id
)
SELECT
/* SEL */
        person_id       person_id,
        group_start     group_start,
        Max (end_date)  group_end,
        COUNT(*)        num_rows
/* SEL */
FROM rsq
GROUP BY person_id, group_start
ORDER BY person_id, group_start
!');

  Bench_Queries.Add_Query (p_query_name => 'MTH_QRY', p_description => 'Match_Recognize', p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */
        person_id       person_id,
        group_start     group_start,
        group_end       group_end,
        num_rows        num_rows
/* SEL */
  FROM activity
 MATCH_RECOGNIZE (
   PARTITION BY person_id
   ORDER BY start_date
   MEASURES FIRST (start_date) group_start,
            FINAL MAX (end_date) group_end,
            COUNT(*) num_rows
      ONE ROW PER MATCH
  PATTERN (strt sm*)
   DEFINE sm AS sm.start_date <= strt.start_date + Sys_Context('bench_ctx', 'deep')
  ) m
ORDER BY person_id, group_start
!');

  DECLARE
    l_ora_vsn   VARCHAR2(10);
  BEGIN

    SELECT Substr (version, 1, Instr (version, '.', 1, 1) - 1)
      INTO l_ora_vsn
      FROM product_component_version
     WHERE product LIKE 'Oracle Database%';
    Utils.Write_Log ('Oracle major version: ' || l_ora_vsn || ' - making 12c MTH_QRY inactive for 8, 9, 10, 11...');

    IF l_ora_vsn IN ('8', '9', '10', '11') THEN

      UPDATE queries
         SET active_yn = 'N'
       WHERE name = 'MTH_QRY'
         AND query_group = c_query_group;

      Utils.Write_Log ('12c MTH_QRY is INactive');

    ELSE

      Utils.Write_Log ('12c MTH_QRY is active');

    END IF;

  END;

END;
/
