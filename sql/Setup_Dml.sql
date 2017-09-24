DROP TABLE product_sales
/
CREATE TABLE product_sales (product_id NUMBER, sales_date DATE)
/
EXEC Utils.Clear_Log;
DECLARE
  c_n_products          PLS_INTEGER := 2;
  c_n_days              PLS_INTEGER := 10;
  c_n_days_in_century   CONSTANT PLS_INTEGER := 36524;
  c_start_date          CONSTANT DATE := DATE '1900-01-01';
BEGIN

  INSERT INTO product_sales
  WITH prod_gen AS (
    SELECT LEVEL rn
      FROM DUAL
      CONNECT BY LEVEL <= c_n_products
  ), day_gen AS (
    SELECT LEVEL rn
      FROM DUAL
      CONNECT BY LEVEL <= c_n_days
  )
  SELECT p.rn, c_start_date + Mod (Abs (DBMS_Random.Random), c_n_days_in_century)
    FROM prod_gen p
   CROSS JOIN day_gen d;

END;
/ 
COMMIT
/
CREATE INDEX ps_date_n1 ON product_sales (sales_date)
/
CREATE INDEX ps_prd_n1 ON product_sales (product_id)
/
SET TIMING ON
COLUMN id FORMAT 999990
COLUMN str FORMAT A40

PROMPT Test Data
SELECT * FROM product_sales
/
SET TIMING ON
PROMPT Update
UPDATE /*+ gather_plan_statistics UPD_DML */ product_sales sd
   SET sd.sales_date = DATE '1900-01-01'
 WHERE 1=1 AND sd.sales_date = ( 
   SELECT Min(sd2.sales_date)
     FROM product_sales sd2
    WHERE sd.product_id = sd2.product_id   
 )
   AND sd.sales_date != DATE '1900-01-01'
/
ROLLBACK
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'UPD_DML', p_add_outline => TRUE);
COMMIT
/
PROMPT Merge swap_join_inputs (TGT is case-sensitive due to "")
MERGE  /*+ gather_plan_statistics MRG_DML swap_join_inputs(@"SEL$F5BB74E1" "TGT"@"SEL$1") */ INTO product_sales tgt
USING (SELECT *
       FROM (
         SELECT rowid arowid, product_id, DATE '1900-01-01' sales_date,
                sales_date AS old_sales_date,
                Rank() OVER (PARTITION BY product_id ORDER BY sales_date) rn
         FROM   product_sales    
       )
       WHERE 1=1 AND rn = 1 AND 0 = Decode(sales_date, old_sales_date, 1, 0)) src
ON (tgt.rowid = src.arowid)
WHEN MATCHED THEN
  UPDATE SET
    tgt.sales_date = src.sales_date
/
ROLLBACK
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'MRG_DML', p_add_outline => TRUE);
PROMPT Merge no_swap_join_inputs
MERGE /*+ gather_plan_statistics MHT_DML no_swap_join_inputs(@"SEL$F5BB74E1" "TGT"@"SEL$1") */ INTO product_sales tgt
USING (SELECT *
       FROM (
         SELECT rowid arowid, product_id, DATE '1900-01-01' sales_date,
                sales_date AS old_sales_date,
                Rank() OVER (PARTITION BY product_id ORDER BY sales_date) rn
         FROM   product_sales    
       )
       WHERE 1=1 AND rn = 1 AND 0 = Decode(sales_date, old_sales_date, 1, 0)) src
ON (tgt.rowid = src.arowid)
WHEN MATCHED THEN
  UPDATE SET
    tgt.sales_date = src.sales_date
/
ROLLBACK
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'MHT_DML', p_add_outline => TRUE);
PROMPT Merge NL
MERGE /*+ gather_plan_statistics MH2_DML leading(@"SEL$F5BB74E1" "from$_subquery$_007"@"SEL$2" "TGT"@"SEL$1") use_nl(@"SEL$F5BB74E1" "TGT"@"SEL$1") */ INTO product_sales tgt
USING (SELECT *
       FROM (
         SELECT rowid arowid, product_id, DATE '1900-01-01' sales_date,
                sales_date AS old_sales_date,
                Rank() OVER (PARTITION BY product_id ORDER BY sales_date) rn
         FROM   product_sales    
       )
       WHERE 1=1 AND rn = 1 AND 0 = Decode(sales_date, old_sales_date, 1, 0)) src
ON (tgt.rowid = src.arowid)
WHEN MATCHED THEN
  UPDATE SET
    tgt.sales_date = src.sales_date
/
ROLLBACK
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'MH2_DML', p_add_outline => TRUE);

PROMPT Delete
DELETE /*+ gather_plan_statistics DEL_DML */ product_sales sd
 WHERE 1=1 AND (product_id, sales_date) IN (
    SELECT product_id, Min(sales_date)
      FROM product_sales
     WHERE 1=1
     GROUP BY product_id
    HAVING Min(sales_date) != DATE '1900-01-01'
    )
/
ROLLBACK
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'DEL_DML', p_add_outline => TRUE);

PROMPT Insert
INSERT INTO product_sales
WITH date_mins AS (
    SELECT product_id
      FROM product_sales
     GROUP BY product_id
    HAVING Min(sales_date) != DATE '1900-01-01'
)
SELECT /*+ gather_plan_statistics INS_DML */ product_id, DATE '1900-01-01'
  FROM date_mins
 WHERE 1=1
/
ROLLBACK
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'INS_DML', p_add_outline => TRUE);
PROMPT CTAS
CREATE TABLE product_sales_ctas AS
SELECT /*+ gather_plan_statistics CTAS_DDL */ product_id,
       CASE WHEN sales_date = Min(sales_date) OVER (PARTITION BY product_id) THEN DATE '1900-01-01' ELSE sales_date END sales_date
  FROM product_sales
  WHERE 1=1
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'CTAS_DDL', p_add_outline => TRUE);

DROP TABLE product_sales
/
RENAME product_sales_ctas TO product_sales
/
CREATE INDEX ps_date_n1 ON product_sales (sales_date)
/
CREATE INDEX ps_prd_n1 ON product_sales (product_id)
/
@..\sql\L_Log_Default
DECLARE
  c_query_group         CONSTANT VARCHAR2(30) := 'DMLSALES';
  c_group_description   CONSTANT VARCHAR2(60) := 'Update/Merge/Insert/Delete product sales';
BEGIN

  Bench_Queries.Add_Query (p_query_name => 'UPD_DML', p_description => 'Update',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
'UPDATE',
        p_pre_query_sql =>
q'!UPDATE product_sales sd
   SET sd.sales_date = DATE '1900-01-01'
 WHERE 1=1 AND sd.sales_date =
 ( SELECT min(sd2.sales_date)
     FROM product_sales sd2
    WHERE sd.product_id = sd2.product_id   
 )
   AND sd.sales_date != DATE '1900-01-01'
!');

  Bench_Queries.Add_Query (p_query_name => 'MRG_DML', p_description => 'Merge',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
'MERGE',
        p_pre_query_sql =>
q'!MERGE INTO product_sales tgt
USING (SELECT *
       FROM (
         SELECT rowid arowid, product_id, DATE '1900-01-01' sales_date,
                sales_date AS old_sales_date,
                Rank() OVER (PARTITION BY product_id ORDER BY sales_date) rn
         FROM   product_sales    
       )
       WHERE 1=1 AND rn = 1 AND 0 = Decode(sales_date, old_sales_date, 1, 0)) src
ON (tgt.rowid = src.arowid)
WHEN MATCHED THEN
  UPDATE SET
    tgt.sales_date = src.sales_date
!');

  Bench_Queries.Add_Query (p_query_name => 'MHT_DML', p_description => 'Merge with no_swap_join_inputs',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
'MERGE',
        p_pre_query_sql =>
q'!MERGE /*+ no_swap_join_inputs(@"SEL$F5BB74E1" "TGT"@"SEL$1") */ INTO product_sales tgt
USING (SELECT *
       FROM (
         SELECT rowid arowid, product_id, DATE '1900-01-01' sales_date,
                sales_date AS old_sales_date,
                Rank() OVER (PARTITION BY product_id ORDER BY sales_date) rn
         FROM   product_sales    
       )
       WHERE 1=1 AND rn = 1 AND 0 = Decode(sales_date, old_sales_date, 1, 0)) src
ON (tgt.rowid = src.arowid)
WHEN MATCHED THEN
  UPDATE SET
    tgt.sales_date = src.sales_date
!');

  Bench_Queries.Add_Query (p_query_name => 'MH2_DML', p_description => 'Merge with use_nl hint',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
'MERGE',
        p_pre_query_sql =>
q'!MERGE /*+ leading(@"SEL$F5BB74E1" "from$_subquery$_007"@"SEL$2" "TGT"@"SEL$1") use_nl(@"SEL$F5BB74E1" "TGT"@"SEL$1") */ INTO product_sales tgt
USING (SELECT *
       FROM (
         SELECT rowid arowid, product_id, DATE '1900-01-01' sales_date,
                sales_date AS old_sales_date,
                Rank() OVER (PARTITION BY product_id ORDER BY sales_date) rn
         FROM   product_sales    
       )
       WHERE 1=1 AND rn = 1 AND 0 = Decode(sales_date, old_sales_date, 1, 0)) src
ON (tgt.rowid = src.arowid)
WHEN MATCHED THEN
  UPDATE SET
    tgt.sales_date = src.sales_date
!');

  Bench_Queries.Add_Query (p_query_name => 'DEL_DML', p_description => 'Delete',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
'DELETE',
        p_pre_query_sql =>
q'!DELETE product_sales sd
  WHERE 1=1 AND (product_id, sales_date) IN (
    SELECT product_id, Min(sales_date)
      FROM product_sales
     WHERE 1=1
     GROUP BY product_id
    HAVING Min(sales_date) != DATE '1900-01-01'
    )
!');

  Bench_Queries.Add_Query (p_query_name => 'INS_DML', p_description => 'Insert',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
'INSERT',
        p_pre_query_sql =>
q'!INSERT INTO product_sales
WITH date_mins AS (
    SELECT product_id
      FROM product_sales
     GROUP BY product_id
     HAVING Min(sales_date) != DATE '1900-01-01'
)
SELECT product_id, DATE '1900-01-01'
  FROM date_mins
 WHERE 1=1
!');

END;
/
DECLARE
  c_query_group         CONSTANT VARCHAR2(30) := 'UPDSALES';
  c_group_description   CONSTANT VARCHAR2(30) := 'Update/merge product sales';
BEGIN

  Bench_Queries.Add_Query (p_query_name => 'UPD_DML', p_description => 'Update',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
'UPDATE',
        p_pre_query_sql =>
q'!UPDATE product_sales sd
   SET sd.sales_date = DATE '1900-01-01'
 WHERE 1=1 AND sd.sales_date = (
   SELECT Min(sd2.sales_date)
     FROM product_sales sd2
    WHERE sd.product_id = sd2.product_id   
 )
   AND sd.sales_date != DATE '1900-01-01'
!');

  Bench_Queries.Add_Query (p_query_name => 'MRG_DML', p_description => 'Merge',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
'MERGE',
        p_pre_query_sql =>
q'!MERGE INTO product_sales tgt
USING (SELECT *
       FROM (
         SELECT rowid arowid, product_id, DATE '1900-01-01' sales_date,
                sales_date AS old_sales_date,
                Rank() OVER (PARTITION BY product_id ORDER BY sales_date) rn
         FROM   product_sales    
       )
       WHERE 1=1 AND rn = 1 AND 0 = Decode(sales_date, old_sales_date, 1, 0)) src
ON (tgt.rowid = src.arowid)
WHEN MATCHED THEN
  UPDATE SET
    tgt.sales_date = src.sales_date
!');

  Bench_Queries.Add_Query (p_query_name => 'MHT_DML', p_description => 'Merge with no_swap_join_inputs',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
'MERGE',
        p_pre_query_sql =>
q'!MERGE /*+ no_swap_join_inputs(@"SEL$F5BB74E1" "TGT"@"SEL$1") */ INTO product_sales tgt
USING (SELECT *
       FROM (
         SELECT rowid arowid, product_id, DATE '1900-01-01' sales_date,
                sales_date AS old_sales_date,
                Rank() OVER (PARTITION BY product_id ORDER BY sales_date) rn
         FROM   product_sales    
       )
       WHERE 1=1 AND rn = 1 AND 0 = Decode(sales_date, old_sales_date, 1, 0)) src
ON (tgt.rowid = src.arowid)
WHEN MATCHED THEN
  UPDATE SET
    tgt.sales_date = src.sales_date
!');

  Bench_Queries.Add_Query (p_query_name => 'MH2_DML', p_description => 'Merge with use_nl hint',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
'MERGE',
        p_pre_query_sql =>
q'!MERGE /*+ leading(@"SEL$F5BB74E1" "from$_subquery$_007"@"SEL$2" "TGT"@"SEL$1") use_nl(@"SEL$F5BB74E1" "TGT"@"SEL$1") */ INTO product_sales tgt
USING (SELECT *
       FROM (
         SELECT rowid arowid, product_id, DATE '1900-01-01' sales_date,
                sales_date AS old_sales_date,
                Rank() OVER (PARTITION BY product_id ORDER BY sales_date) rn
         FROM   product_sales    
       )
       WHERE 1=1 AND rn = 1 AND 0 = Decode(sales_date, old_sales_date, 1, 0)) src
ON (tgt.rowid = src.arowid)
WHEN MATCHED THEN
  UPDATE SET
    tgt.sales_date = src.sales_date
!');

END;
/
DECLARE
  c_query_group         CONSTANT VARCHAR2(30) := 'DDLSALES';
  c_group_description   CONSTANT VARCHAR2(60) := 'Create product sales date index';
BEGIN

  Bench_Queries.Add_Query (p_query_name => 'SDT_DDL', p_description => 'Create sales_date index',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text => 'DDL',
        p_pre_query_sql => 'CREATE INDEX ps_date_n1 ON product_sales (sales_date)',
        p_post_query_sql => 'DROP INDEX ps_date_n1');

  Bench_Queries.Add_Query (p_query_name => 'PRD_DDL', p_description => 'Create product_id index',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text => 'DDL',
        p_pre_query_sql => 'CREATE INDEX ps_prd_n1 ON product_sales (product_id)',
        p_post_query_sql => 'DROP INDEX ps_prd_n1');

  Bench_Queries.Add_Query (p_query_name => 'CRE_DDL', p_description => 'Create table as select',
        p_active_yn => 'Y', p_v12_active_only => FALSE, p_query_group => c_query_group, p_group_description => c_group_description, p_text => 'CREATE',
        p_pre_query_sql => 'CREATE TABLE product_sales_ctas AS 
SELECT product_id,
       CASE WHEN sales_date = Min(sales_date) OVER (PARTITION BY product_id) THEN DATE ''2017-01-01'' ELSE sales_date END sales_date
  FROM product_sales
  WHERE 1=1',
        p_post_query_sql => 'DROP TABLE product_sales_ctas');

END;
/
