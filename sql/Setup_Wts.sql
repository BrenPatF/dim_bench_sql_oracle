DROP TABLE items
/
CREATE TABLE items (
    id          NUMBER, 
    cat         VARCHAR2(30), 
    seq         NUMBER, 
    weight      NUMBER,
    CONSTRAINT itm_pk PRIMARY KEY (ID)
)
/
CREATE INDEX items_N1 ON items (cat, seq)
/
DROP  SEQUENCE itm_s
/
CREATE SEQUENCE itm_s
/
DROP TABLE items_tmp
/
CREATE GLOBAL TEMPORARY TABLE items_tmp (
    id          NUMBER, 
    cat         VARCHAR2(30), 
    seq         NUMBER, 
    weight      NUMBER,
    itm_rownum  NUMBER
)
ON COMMIT DELETE ROWS
/
CREATE INDEX items_tmp_N1 ON items_tmp (itm_rownum, cat)
/
DECLARE

  g_cat                 VARCHAR2(30);
  g_items               VARCHAR2(10) := 'LEAVE';

  PROCEDURE Add_itm (p_seq         NUMBER, 
                     p_weight     NUMBER) IS
  BEGIN

    INSERT INTO items (
        id,
        cat, 
        seq, 
        weight
    ) VALUES (
        itm_s.NEXTVAL,
        g_cat, 
        p_seq, 
        p_weight
    );

  END Add_itm;

BEGIN

  g_cat := 'Rural';

  Add_itm (10,	2);
  Add_itm (9,	3);
  Add_itm (8,	1);
  Add_itm (7,	4);
  Add_itm (6,	11);
  Add_itm (5,	2);
  Add_itm (4,	2);
  Add_itm (3,	4);
  Add_itm (2,	30);
  Add_itm (1,	12);

  g_cat := 'Urban';

  Add_itm (10,	1);
  Add_itm (9,	12);
  Add_itm (8,	2);
  Add_itm (7,	5);
  Add_itm (6,	7);
  Add_itm (5,	15);
  Add_itm (4,	25);
  Add_itm (3,	2);
  Add_itm (2,	1);
  Add_itm (1,	8);

  g_cat := 'Suburban';

  Add_itm (10,	2);
  Add_itm (9,	1);

END;
/
COLUMN id FORMAT 90
EXEC Utils.Clear_Log;
VAR DEPTH NUMBER
EXEC :DEPTH := 10;

SET TIMING ON
PROMPT Group counts via model for limiting record in next group
SELECT cat, 
        Count (DISTINCT final_grp) "# Groups"
  FROM (
SELECT  id,
        cat, 
        seq, 
        weight,
        sub_weight,
        final_grp
  FROM items
 MODEL
    PARTITION BY (cat)
    DIMENSION BY (Row_Number() OVER (PARTITION BY cat ORDER BY seq DESC) rn)
    MEASURES (id, weight, weight sub_weight, id final_grp, seq)
    RULES AUTOMATIC ORDER (
       sub_weight[rn > 1] = CASE WHEN sub_weight[cv()-1] + weight[cv()] > :DEPTH THEN weight[cv()] ELSE sub_weight[cv()-1] + weight[cv()] END,
       final_grp[ANY] = PRESENTV (final_grp[cv()+1], CASE WHEN sub_weight[cv()-1] + weight[cv()] > :DEPTH THEN id[cv()] ELSE final_grp[cv()+1] END, id[cv()])
    )
)
 GROUP BY cat
 ORDER BY 1
/
PROMPT Group counts via model where group is defined as first id in group not last
SELECT cat, 
        Count (DISTINCT final_grp) "# Groups"
  FROM (
SELECT  id,
        cat, 
        seq, 
        weight,
        sub_weight,
        final_grp
  FROM items
 MODEL
    PARTITION BY (cat)
    DIMENSION BY (Row_Number() OVER (PARTITION BY cat ORDER BY seq DESC) rn)
    MEASURES (id, weight, weight sub_weight, id final_grp, seq)
    RULES SEQUENTIAL ORDER (
       sub_weight[rn > 1] = CASE WHEN sub_weight[cv()-1] >= :DEPTH THEN weight[cv()] ELSE sub_weight[cv()-1] + weight[cv()] END,
       final_grp[rn > 1] = CASE WHEN sub_weight[cv()] >= sub_weight[cv()-1] AND sub_weight[cv()] < :DEPTH THEN final_grp[cv()-1] ELSE id[cv()] END
    )
)
 GROUP BY cat
 ORDER BY 1
/
PROMPT Group counts via model
SELECT cat, 
        Count (DISTINCT final_grp) "# Groups"
  FROM (
SELECT  id,
        cat, 
        seq, 
        weight,
        sub_weight,
        final_grp
  FROM items
 MODEL
    PARTITION BY (cat)
    DIMENSION BY (Row_Number() OVER (PARTITION BY cat ORDER BY seq DESC) rn)
    MEASURES (id, weight, weight sub_weight, id final_grp, seq)
    RULES AUTOMATIC ORDER (
       sub_weight[rn > 1] = CASE WHEN sub_weight[cv()-1] >= :DEPTH THEN weight[cv()] ELSE sub_weight[cv()-1] + weight[cv()] END,
       final_grp[ANY] = PRESENTV (final_grp[cv()+1], CASE WHEN sub_weight[cv()] >= :DEPTH THEN id[cv()] ELSE final_grp[cv()+1] END, id[cv()])
    )
)
 GROUP BY cat
 ORDER BY 1
/

BREAK ON cat SKIP 1
PROMPT Model - All records
SELECT /*+ MOD_QRY gather_plan_statistics */
        id,
        cat, 
        seq, 
        weight,
        sub_weight,
        final_grp
  FROM items
 MODEL
    PARTITION BY (cat)
    DIMENSION BY (Row_Number() OVER (PARTITION BY cat ORDER BY seq DESC) rn)
    MEASURES (id, weight, weight sub_weight, id final_grp, seq)
    RULES AUTOMATIC ORDER (
       sub_weight[rn > 1] = CASE WHEN sub_weight[cv()-1] >= :DEPTH THEN weight[cv()] ELSE sub_weight[cv()-1] + weight[cv()] END,
       final_grp[ANY] = PRESENTV (final_grp[cv()+1], CASE WHEN sub_weight[cv()] >= :DEPTH THEN id[cv()] ELSE final_grp[cv()+1] END, id[cv()])
    )
ORDER BY 1, 2, 3
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'MOD_QRY');

PROMPT Recursive Subquery Factor - Groups
WITH itm AS (
SELECT id, cat, seq, weight, Row_Number() OVER (PARTITION BY cat ORDER BY seq DESC) rn
  FROM items
), rsq (id, cat, rn, seq, weight, sub_weight, grp_num) AS (
SELECT id, cat, rn, seq, weight, weight, 1
  FROM itm
 WHERE rn = 1
 UNION ALL
SELECT  itm.id,
        itm.cat,
        itm.rn,
        itm.seq,
        itm.weight,
        itm.weight + CASE WHEN rsq.sub_weight >= :DEPTH THEN 0 ELSE rsq.sub_weight END,    
        RSQ.grp_num + CASE WHEN rsq.sub_weight >= :DEPTH THEN 1 ELSE 0 END
  FROM rsq
  JOIN itm
    ON itm.rn        = rsq.rn + 1
   AND itm.cat       = rsq.cat
)
SELECT /*+ RSF_QRY gather_plan_statistics */
        id,
        cat             cat,
        seq,
        weight         weight,
        sub_weight     sub_weight,
        FIRST_VALUE(id) OVER (PARTITION BY cat, grp_num ORDER BY seq) final_grp
FROM rsq
ORDER BY 1
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'RSF_QRY');
SET TIMING OFF
DECLARE
  c_query_group         CONSTANT VARCHAR2(30) := 'WEIGHTS';
  c_group_description   CONSTANT VARCHAR2(30) := 'Item Weights';
BEGIN

  Bench_Queries.Add_Query (p_query_name => 'MOD_QRY_D', p_description => 'Model clause, sequential, descending', p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
WITH all_rows AS (  
SELECT  id,
        cat, 
        seq, 
        weight,
        sub_weight,
        final_grp
  FROM items
 MODEL
    PARTITION BY (cat)
    DIMENSION BY (Row_Number() OVER (PARTITION BY cat ORDER BY seq DESC) rn)
    MEASURES (id, weight, weight sub_weight, id final_grp, seq)
    RULES (
       sub_weight[rn > 1] = CASE WHEN sub_weight[cv()-1] >= 5000 THEN weight[cv()] ELSE sub_weight[cv()-1] + weight[cv()] END,
       final_grp[ANY] ORDER BY rn DESC = PRESENTV (final_grp[cv()+1], CASE WHEN sub_weight[cv()] >= 5000 THEN id[cv()] ELSE final_grp[cv()+1] END, id[cv()])
    )
)
SELECT 
/* SEL */
        cat             cat,
        final_grp       final_grp,
        COUNT(*)        num_rows
/* SEL */
  FROM all_rows
GROUP BY cat, final_grp
ORDER BY cat, final_grp
!');

  Bench_Queries.Add_Query (p_query_name => 'MOD_QRY', p_description => 'Model clause', p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
WITH all_rows AS (  
SELECT  id,
        cat, 
        seq, 
        weight,
        sub_weight,
        final_grp
  FROM items
 MODEL
    PARTITION BY (cat)
    DIMENSION BY (Row_Number() OVER (PARTITION BY cat ORDER BY seq DESC) rn)
    MEASURES (id, weight, weight sub_weight, id final_grp, seq)
    RULES AUTOMATIC ORDER (
       sub_weight[rn > 1] = CASE WHEN sub_weight[cv()-1] >= 5000 THEN weight[cv()] ELSE sub_weight[cv()-1] + weight[cv()] END,
       final_grp[ANY] = PRESENTV (final_grp[cv()+1], CASE WHEN sub_weight[cv()] >= 5000 THEN id[cv()] ELSE final_grp[cv()+1] END, id[cv()])
    )
)
SELECT 
/* SEL */
        cat             cat,
        final_grp       final_grp,
        COUNT(*)        num_rows
/* SEL */
  FROM all_rows
GROUP BY cat, final_grp
ORDER BY cat, final_grp
!');

  Bench_Queries.Add_Query (p_query_name => 'RSF_QRY', p_description => 'Recursive subquery factoring', p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
WITH itm AS (
SELECT id, cat, seq, weight, Row_Number() OVER (PARTITION BY cat ORDER BY seq DESC) rn
  FROM items
), rsq (id, cat, rn, seq, weight, sub_weight, grp_num) AS (
SELECT id, cat, rn, seq, weight, weight, 1
  FROM itm
 WHERE rn = 1
 UNION ALL
SELECT  itm.id,
        itm.cat,
        itm.rn,
        itm.seq,
        itm.weight,
        itm.weight + CASE WHEN rsq.sub_weight >= 5000 THEN 0 ELSE rsq.sub_weight END,    
        rsq.grp_num + CASE WHEN rsq.sub_weight >= 5000 THEN 1 ELSE 0 END
  FROM rsq
  JOIN itm
    ON itm.rn        = rsq.rn + 1
   AND itm.cat       = rsq.cat
), final_grouping AS (
SELECT
        cat             cat,
        First_Value(id) OVER (PARTITION BY cat, grp_num ORDER BY seq) final_grp
FROM rsq
)
SELECT
/* SEL */
        cat             cat,
        final_grp       final_grp,
        COUNT(*)        num_rows
/* SEL */
FROM final_grouping
GROUP BY cat, final_grp
ORDER BY cat, final_grp
!');

  Bench_Queries.Add_Query (p_query_name => 'RSF_TMP', p_description => 'Recursive subquery factoring, with temp table', p_active_yn => 'Y', p_query_group => c_query_group, 
        p_group_description => c_group_description, 
        p_pre_query_sql => q'!INSERT INTO items_tmp SELECT id, cat, seq, weight, Row_Number() OVER (PARTITION BY cat ORDER BY seq DESC) FROM items!',
        p_text =>
q'!
WITH rsq (id, cat, rn, seq, weight, sub_weight, grp_num) AS (
SELECT id, cat, itm_rownum, seq, weight, weight, 1
  FROM items_tmp
 WHERE itm_rownum = 1
 UNION ALL
SELECT  /*+ INDEX (itm items_tmp_n1) */ itm.id, 
        itm.cat,
        itm.itm_rownum,
        itm.seq,
        itm.weight,
        itm.weight + CASE WHEN rsq.sub_weight >= 5000 THEN 0 ELSE rsq.sub_weight END,    
        rsq.grp_num + CASE WHEN rsq.sub_weight >= 5000 THEN 1 ELSE 0 END
  FROM rsq
  JOIN items_tmp itm
    ON itm.itm_rownum   = rsq.rn + 1
   AND itm.cat          = rsq.cat
), final_grouping AS (
SELECT
        cat             cat,
        First_Value(id) OVER (PARTITION BY cat, grp_num ORDER BY seq) final_grp
FROM rsq
)
SELECT
/* SEL */
        cat             cat,
        final_grp       final_grp,
        COUNT(*)        num_rows
/* SEL */
FROM final_grouping
GROUP BY cat, final_grp
ORDER BY cat, final_grp
!');

  Bench_Queries.Add_Query (p_query_name => 'MTH_QRY', p_description => 'Match_Recognize',
        p_active_yn => 'Y', p_v12_active_only => true, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */
        cat         cat,
        final_grp   final_grp,
        num_rows    num_rows
/* SEL */
  FROM items
 MATCH_RECOGNIZE (
   PARTITION BY cat
   ORDER BY seq DESC
   MEASURES FINAL LAST (id) final_grp,
            COUNT(*) num_rows
   ONE ROW PER MATCH
   PATTERN (s* t?)
   DEFINE s AS Sum (weight) < 5000,
          t AS Sum (weight) >= 5000
 ) m
ORDER BY cat, final_grp
!');

END;
/

