DROP TYPE org_struct_lis_type;
CREATE OR REPLACE TYPE org_struct_rec_type IS OBJECT (struct_level NUMBER, org_id NUMBER, fact_product NUMBER);
/
CREATE TYPE org_struct_lis_type IS VARRAY(32767) OF org_struct_rec_type;
/

DROP TABLE orgs
/
CREATE TABLE orgs ( id              NUMBER NOT NULL, 
                    org_level       NUMBER NOT NULL, 
                    org_name        VARCHAR2(100) NOT NULL,
                    CONSTRAINT      org_pk PRIMARY KEY (id))
/
DROP TABLE org_structure
/
CREATE TABLE org_structure (
                    id              NUMBER NOT NULL, 
                    struct_level    NUMBER NOT NULL, 
                    org_id          NUMBER NOT NULL, 
                    child_org_id    NUMBER NOT NULL,
                    fact            NUMBER,
                    CONSTRAINT      ost_pk PRIMARY KEY (id))
/
CREATE INDEX ost_N1 ON org_structure (org_id)
/
CREATE INDEX ost_N2 ON org_structure (child_org_id)
/
CREATE OR REPLACE FUNCTION Org_Products (p_org_id PLS_INTEGER, p_fact_product NUMBER) RETURN org_struct_lis_type PIPELINED IS
  l_org_struct_lis  org_struct_lis_type;
BEGIN

  FOR rec_org_struct IN (
      SELECT child_org_id,
             p_fact_product * fact fact_product,
             struct_level
      FROM org_structure
      WHERE org_id = p_org_id) LOOP

    PIPE ROW (org_struct_rec_type (rec_org_struct.struct_level, rec_org_struct.child_org_id, rec_org_struct.fact_product));

    FOR rec_org_struct_child IN (SELECT struct_level, org_id, fact_product FROM TABLE (Org_Products (rec_org_struct.child_org_id, rec_org_struct.fact_product))) LOOP

      PIPE ROW (org_struct_rec_type (rec_org_struct_child.struct_level, rec_org_struct_child.org_id, rec_org_struct_child.fact_product));

    END LOOP;

  END LOOP;

END  Org_Products;
/
SHO ERR
DECLARE

  l_num_records          PLS_INTEGER;
  l_num_records_per_part PLS_INTEGER;
  l_group_size           PLS_INTEGER;
  l_text                 VARCHAR2(100);

PROCEDURE Setup_Data_Org (p_point_wide                PLS_INTEGER, -- wide data point
                          p_point_deep                PLS_INTEGER, -- deep data point
                          x_num_records           OUT PLS_INTEGER, -- number of records created
                          x_num_records_per_part  OUT NUMBER,      -- number of records created per partition key
                          x_group_size            OUT NUMBER,      -- group size, where applicable
                          x_text                  OUT VARCHAR2) IS -- descriptive text about the data set

  l_base_org                     VARCHAR2(4000);
  l_org_wide                     VARCHAR2(4000);

  c_n_top_level_orgs             CONSTANT PLS_INTEGER := 3;
  c_n_levels                     CONSTANT PLS_INTEGER := 3;
  c_deep_factor                  CONSTANT NUMBER := 0.001;
  l_timer                        PLS_INTEGER;
  l_org_id                       PLS_INTEGER := 0;
  l_ost_id                       PLS_INTEGER := 0;
  l_parent_org_id_min            PLS_INTEGER;
  l_parent_org_id_max            PLS_INTEGER;
  l_lev_orgs                     PLS_INTEGER := c_n_top_level_orgs;

  l_first_org_lis                L1_num_arr := L1_num_arr ();
  PROCEDURE Ins_Org (p_level PLS_INTEGER, p_org_name VARCHAR2) IS
  BEGIN

    l_org_id := l_org_id + 1;
    INSERT INTO orgs VALUES (l_org_id, p_level, p_org_name);
    
  END Ins_Org;

  PROCEDURE Ins_Ost (p_level PLS_INTEGER, p_org_id PLS_INTEGER, p_child_org_id PLS_INTEGER) IS
  BEGIN

    l_ost_id := l_ost_id + 1;
    INSERT INTO org_structure VALUES (l_ost_id, p_level, p_org_id, p_child_org_id, DBMS_Random.Value);
    
  END Ins_Ost;

BEGIN

  l_timer := Timer_Set.Construct ('Setup');

  l_first_org_lis.EXTEND (c_n_levels + 1);
  FOR i IN 1..c_n_levels LOOP

    l_first_org_lis(i) := l_org_id + 1;
    FOR j IN 1..l_lev_orgs LOOP

      Ins_Org (i, 'L' || i || ' Org ' || j);

    END LOOP;
    l_lev_orgs := l_lev_orgs * (1 + p_point_wide/100);

  END LOOP;
  l_first_org_lis(c_n_levels + 1) := l_org_id + 1;

  FOR i IN REVERSE 2..c_n_levels LOOP

    l_parent_org_id_min := l_first_org_lis(i-1);
    l_parent_org_id_max := l_first_org_lis(i) - 1;
    DBMS_Output.Put_Line (i || ': ' || l_parent_org_id_min || ', ' || l_parent_org_id_max);
    FOR j IN l_first_org_lis(i)..l_first_org_lis(i+1) - 1 LOOP

--      DBMS_Output.Put_Line ('KMAX = ' || (c_deep_factor * p_point_deep * (l_parent_org_id_max - l_parent_org_id_min + 1)));
      FOR k IN 1..Greatest (1, Round (c_deep_factor * p_point_deep * (l_parent_org_id_max - l_parent_org_id_min + 1))) LOOP

        Ins_Ost (i-1, 
                 l_parent_org_id_min + DBMS_Random.Value * (l_parent_org_id_max - l_parent_org_id_min),
                 j);

      END LOOP;

    END LOOP;
    l_lev_orgs := l_lev_orgs * (1 + p_point_wide/100);

  END LOOP;

/*
  x_num_records := c_num_recs;
  x_num_records_per_part := c_num_recs;
  x_group_size := c_num_recs;
*/
x_text := 'Org structures';

END Setup_Data_Org;

BEGIN

  Setup_Data_Org (p_point_wide           => 100,
                  p_point_deep           => 300,
                  x_num_records          => l_num_records,
                  x_num_records_per_part => l_num_records_per_part,
                  x_group_size           => l_group_size,
                  x_text                 => l_text);
END;
/
BREAK ON org_level
PROMPT orgs
SELECT *
  FROM orgs
 ORDER BY id
/
PROMPT org_structure
BREAK ON struct_level
SELECT *
  FROM org_structure
 ORDER BY struct_level, id
/
COLUMN root_org FORMAT A10
COLUMN L2_org FORMAT A10
COLUMN leaf_org FORMAT A10
COLUMN fact_product FORMAT 0.90
BREAK ON L1_org ON L2_org
PROMPT Joins
SELECT  /*+ JNS_QRY gather_plan_statistics */
       o1.org_name root_org,
       o3.org_name leaf_org,
       s1.fact * s2.fact fact_product
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o2
    ON o2.id = s2.org_id
  JOIN orgs o3
    ON o3.id = s2.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY 1, 2, 3
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'JNS_QRY');
PROMPT Recursive subquery factor

WITH rsf (root_org_id, child_org_id, fact_product, lev) AS
(
SELECT org_id, child_org_id, fact, 1
  FROM org_structure
 WHERE struct_level = 1
UNION ALL
SELECT r.root_org_id,
       s.child_org_id,
       r.fact_product * s.fact,
       r.lev + 1
  FROM rsf r
  JOIN org_structure s
    ON s.org_id = r.child_org_id
)
SELECT /*+ RSF_QRY gather_plan_statistics */
       o1.org_name root_org,
       o3.org_name leaf_org,
       fact_product
  FROM rsf r
  JOIN orgs o1
    ON o1.id = r.root_org_id
  JOIN orgs o3
    ON o3.id = r.child_org_id
 WHERE r.lev = 2
 ORDER BY 1, 2
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'RSF_QRY');

PROMPT Pipelined function main

SELECT /*+ PLF_QRY gather_plan_statistics */
       o1.org_name root_org,
       o3.org_name leaf_org,
       t.fact_product
  FROM org_structure s
  CROSS APPLY TABLE (Org_Products (s.child_org_id, s.fact)) t
  JOIN orgs o1
    ON o1.id = s.org_id
  JOIN orgs o3
    ON o3.id = t.org_id
 WHERE s.struct_level = 1
   AND t.struct_level = 2
 ORDER BY 1, 2
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'PLF_QRY');

PROMPT Pipelined function internal query

SELECT /*+ PLF_INT_QRY gather_plan_statistics */
       child_org_id,
       fact fact_product,
       struct_level
  FROM org_structure
  WHERE org_id = 1
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'PLF_INT_QRY');
SET TIMING OFF

DECLARE
  c_query_group         CONSTANT VARCHAR2(30) := 'ORG_STRUCT';
  c_group_description   CONSTANT VARCHAR2(30) := 'Org structures';
BEGIN

  Bench_Queries.Add_Query (p_query_name => 'JNS_QRY', p_description => 'Join sequence',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'RSF_QRY', p_description => 'Recursive subquery factors', p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
WITH rsf (root_org_id, child_org_id, fact_product, lev) AS
(
SELECT org_id, child_org_id, fact, 1
  FROM org_structure
 WHERE struct_level = 1
UNION ALL
SELECT r.root_org_id,
       s.child_org_id,
       r.fact_product * s.fact,
       r.lev + 1
  FROM rsf r
  JOIN org_structure s
    ON s.org_id = r.child_org_id
)
SELECT
/* SEL */
       o1.org_name root_org,
       o5.org_name leaf_org,
       r.fact_product fact_product
/* SEL */
  FROM rsf r
  JOIN orgs o1
    ON o1.id = r.root_org_id
  JOIN orgs o5
    ON o5.id = r.child_org_id
 WHERE r.lev = 4
 ORDER BY o1.org_name, o5.org_name, r.fact_product
!');

  Bench_Queries.Add_Query (p_query_name => 'PLF_QRY', p_description => 'Pipelined function', p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */
       o1.org_name root_org,
       o5.org_name leaf_org,
       t.fact_product fact_product
/* SEL */
  FROM org_structure s
  CROSS APPLY TABLE (Org_Products (s.child_org_id, s.fact)) t
  JOIN orgs o1
    ON o1.id = s.org_id
  JOIN orgs o5
    ON o5.id = t.org_id
 WHERE s.struct_level = 1
   AND t.struct_level = 4
 ORDER BY o1.org_name, o5.org_name, t.fact_product
!');

END;
/

DECLARE
  c_query_group         CONSTANT VARCHAR2(30) := 'ORG_HINTS';
  c_group_description   CONSTANT VARCHAR2(30) := 'Org structures, with hints';
BEGIN

  Bench_Queries.Add_Query (p_query_name => 'H01_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5) no_swap_join_inputs(s2) no_swap_join_inputs(s3) no_swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H02_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5) no_swap_join_inputs(s2) no_swap_join_inputs(s3) no_swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H03_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5)    swap_join_inputs(s2) no_swap_join_inputs(s3) no_swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H04_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5)    swap_join_inputs(s2) no_swap_join_inputs(s3) no_swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H05_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5) no_swap_join_inputs(s2)    swap_join_inputs(s3) no_swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H06_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5) no_swap_join_inputs(s2)    swap_join_inputs(s3) no_swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H07_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5)    swap_join_inputs(s2)    swap_join_inputs(s3) no_swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H08_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5)    swap_join_inputs(s2)    swap_join_inputs(s3) no_swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H09_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5) no_swap_join_inputs(s2) no_swap_join_inputs(s3)    swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H10_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5) no_swap_join_inputs(s2) no_swap_join_inputs(s3)    swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H11_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5)    swap_join_inputs(s2) no_swap_join_inputs(s3)    swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H12_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5)    swap_join_inputs(s2) no_swap_join_inputs(s3)    swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H13_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5) no_swap_join_inputs(s2)    swap_join_inputs(s3)    swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H14_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5) no_swap_join_inputs(s2)    swap_join_inputs(s3)    swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H15_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5)    swap_join_inputs(s2)    swap_join_inputs(s3)    swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H16_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5)    swap_join_inputs(s2)    swap_join_inputs(s3)    swap_join_inputs(s4) no_swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H17_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5) no_swap_join_inputs(s2) no_swap_join_inputs(s3) no_swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H18_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5) no_swap_join_inputs(s2) no_swap_join_inputs(s3) no_swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H19_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5)    swap_join_inputs(s2) no_swap_join_inputs(s3) no_swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H20_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5)    swap_join_inputs(s2) no_swap_join_inputs(s3) no_swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H21_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5) no_swap_join_inputs(s2)    swap_join_inputs(s3) no_swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H22_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5) no_swap_join_inputs(s2)    swap_join_inputs(s3) no_swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H23_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5)    swap_join_inputs(s2)    swap_join_inputs(s3) no_swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H24_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5)    swap_join_inputs(s2)    swap_join_inputs(s3) no_swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H25_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5) no_swap_join_inputs(s2) no_swap_join_inputs(s3)    swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H26_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5) no_swap_join_inputs(s2) no_swap_join_inputs(s3)    swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H27_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5)    swap_join_inputs(s2) no_swap_join_inputs(s3)    swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H28_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5)    swap_join_inputs(s2) no_swap_join_inputs(s3)    swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H29_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5) no_swap_join_inputs(s2)    swap_join_inputs(s3)    swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H30_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5) no_swap_join_inputs(s2)    swap_join_inputs(s3)    swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H31_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(s1 o1 s2 s3 s4) use_hash(o1 s2 s3 s4 o5)    swap_join_inputs(s2)    swap_join_inputs(s3)    swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

  Bench_Queries.Add_Query (p_query_name => 'H32_QRY', p_description => 'Join sequence, hints sequence of 32',
        p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT
/* SEL */ /*+ leading(o1 s1 s2 s3 s4) use_hash(s1 s2 s3 s4 o5)    swap_join_inputs(s2)    swap_join_inputs(s3)    swap_join_inputs(s4)    swap_join_inputs(o5) */
       o1.org_name root_org,
       o5.org_name leaf_org,
       s1.fact * s2.fact * s3.fact * s4.fact fact_product
/* SEL */
  FROM org_structure s1
  JOIN org_structure s2
    ON s2.org_id = s1.child_org_id
  JOIN org_structure s3
    ON s3.org_id = s2.child_org_id
  JOIN org_structure s4
    ON s4.org_id = s3.child_org_id
  JOIN orgs o1
    ON o1.id = s1.org_id
  JOIN orgs o5
    ON o5.id = s4.child_org_id
 WHERE s1.struct_level = 1
 ORDER BY o1.org_name, o5.org_name, s1.fact * s2.fact * s3.fact * s4.fact
!');

END;
/