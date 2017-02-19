DROP TABLE activity
/
CREATE TABLE activity (
    activity_id     NUMBER, 
    person_id       NUMBER, 
    start_date      DATE, 
    end_date        DATE, 
    activity_name   VARCHAR2(10)
)
/
CREATE INDEX activity_N1 ON activity (person_id, start_date, Nvl (end_date, '01-JAN-3000'))
/
CREATE INDEX activity_N2 ON activity (person_id, Nvl (end_date, '01-JAN-3000'), start_date)
/
DROP  SEQUENCE act_s
/
CREATE SEQUENCE act_s
/
DROP TABLE activity_tmp
/
CREATE GLOBAL TEMPORARY TABLE activity_tmp (
  person_id     NUMBER,
  start_date    DATE,
  end_date      DATE, 
  act_rownum    NUMBER
)
ON COMMIT DELETE ROWS
/
CREATE INDEX activity_tmp_N1 ON activity_tmp (act_rownum, person_id)
/
DECLARE

  g_person_id       NUMBER := 1;
  g_activity        VARCHAR2(10) := 'LEAVE';

  PROCEDURE Add_Act (p_start_day DATE, p_days PLS_INTEGER DEFAULT NULL) IS
  BEGIN

    INSERT INTO activity (
        activity_id,
        person_id,    
        start_date,    
        end_date,    
        activity_name
    ) VALUES (
        act_s.NEXTVAL,
        g_person_id,
        p_start_day,
        CASE WHEN p_days IS NULL THEN To_Date(NULL) ELSE p_start_day+p_days END,
        g_activity
    );

  END ;

BEGIN

-- Overlapping, but with no extra break
    -- Case 3. 3 records (with overlaps), gap, 2 records (second enclosed by first), gap, 1 record

  g_person_id := 3;
  g_activity := 'LEAVE';

  Add_Act ('01-JUN-2011', 2);
  Add_Act ('02-JUN-2011', 3);
  Add_Act ('04-JUN-2011', 3);

  Add_Act ('08-JUN-2011', 8);
  g_activity := 'TRAINING';
  Add_Act ('09-JUN-2011', 5);

  Add_Act ('20-JUN-2011', 10);

    -- Case 4. 3 records (with overlaps), gap, 3 records, second overlaps first, with null end date

  g_person_id := 4;
  g_activity := 'LEAVE';

  Add_Act ('01-JUN-2011', 2);
  Add_Act ('02-JUN-2011', 3);
  Add_Act ('04-JUN-2011', 3);

  Add_Act ('08-JUN-2011', 8);
  g_activity := 'TRAINING';
  Add_Act ('09-JUN-2011', 6);

  Add_Act ('20-JUN-2011', 10);

    -- Case 5. 3 records (with overlaps), gap, 2 records (second enclosed by first), gap but not with respect to first, 1 record

  g_person_id := 5;
  g_activity := 'LEAVE';

  Add_Act ('01-JUN-2011', 2);
  Add_Act ('02-JUN-2011', 3);
  Add_Act ('04-JUN-2011', 3);

  Add_Act ('08-JUN-2011', 8);
  g_activity := 'TRAINING';
  Add_Act ('09-JUN-2011', 5);

  Add_Act ('15-JUN-2011', 15);

END;
/
COLUMN id FORMAT 90
EXEC Utils.Clear_Log;
VAR DEPTH NUMBER
EXEC :DEPTH := 3;

SET TIMING ON
PROMPT Group counts via model
SELECT /*+ GRP_CNT gather_plan_statistics */
       person_id, Count (DISTINCT group_start) "# Groups"
  FROM (
SELECT person_id, start_date, end_date, group_start
  FROM activity
 MODEL
    PARTITION BY (person_id)
    DIMENSION BY (Row_Number() OVER (PARTITION BY person_id ORDER BY start_date) rn)
    MEASURES (start_date, end_date, start_date group_start)
    RULES (
       group_start[rn = 1] = start_date[cv()],
       group_start[rn > 1] = CASE WHEN start_date[cv()] - group_start[cv()-1] > :DEPTH THEN start_date[cv()] ELSE group_start[cv()-1] END
    )
)
 GROUP BY person_id
 ORDER BY 1
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'GRP_CNT');

BREAK ON person_id SKIP 1
PROMPT Model - All records
WITH mod AS (
SELECT person_id, start_date, end_date, group_start
  FROM activity
 MODEL
    PARTITION BY (person_id)
    DIMENSION BY (Row_Number() OVER (PARTITION BY person_id ORDER BY start_date) rn)
    MEASURES (start_date, end_date, start_date group_start, end_date group_end)
    RULES (
       group_start[rn > 1] = CASE WHEN start_date[cv()] - group_start[cv()-1] > :DEPTH THEN start_date[cv()] ELSE group_start[cv()-1] END,
       group_end[ANY] ORDER BY rn DESC = PRESENTV (group_start[cv()+1],
            CASE WHEN group_start[cv()] < group_start[cv()+1] THEN end_date[cv()] ELSE group_end[cv()+1] END,
            end_date[cv()])
    )
)
SELECT /*+ MOD gather_plan_statistics */ 
        person_id, start_date, end_date, group_start,
        Max(end_date) KEEP (DENSE_RANK LAST ORDER BY end_date NULLS LAST) OVER (PARTITION BY person_id, group_start) group_end
  FROM mod
ORDER BY 1, 2, 3
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'MOD');

PROMPT Recursive Subquery Factor - Groups
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
        CASE WHEN act.start_date - rsq.group_start <= :DEPTH THEN rsq.group_start ELSE act.start_date end
  FROM act
  JOIN rsq
    ON rsq.rn              = act.rn - 1
   AND rsq.person_id       = act.person_id
)
SELECT /*+ RSF gather_plan_statistics */
        person_id       person_id,
        group_start     group_start,
        Max (end_date)  group_end,
        COUNT(*)        num_rows
FROM rsq
GROUP BY person_id, group_start
ORDER BY person_id, group_start
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'RSF');
SET TIMING OFF

DECLARE
  c_query_group     CONSTANT VARCHAR2(30) := 'BURST';
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

  Bench_Queries.Add_Query (p_query_name => 'MTH_QRY', p_description => 'Match_Recognize', 
        p_active_yn => 'Y', p_v12_active_only => TRUE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
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

END;
/

