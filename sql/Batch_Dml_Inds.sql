SET TRIMSPOOL ON
SPOOL ..\out\Batch_Dml_Inds

COLUMN "Database"       FORMAT A20
COLUMN "Time"           FORMAT A20
COLUMN "Version"        FORMAT A30
COLUMN "Session"        FORMAT 9999990
COLUMN "OS User"        FORMAT A10
COLUMN "Machine"        FORMAT A20
SET LINES 180
SET PAGES 1000

SELECT 'Start: ' || dbs.name "Database", To_Char (SYSDATE,'DD-MON-YYYY HH24:MI:SS') "Time",
        Replace (Substr(ver.banner, 1, Instr(ver.banner, '64')-4), 'Enterprise Edition Release ', '') "Version"
  FROM v$database dbs,  v$version ver
 WHERE ver.banner LIKE 'Oracle%';

DEFINE RUNDESC='Ups-One'

SET SERVEROUTPUT ON
SET TIMING ON

CREATE INDEX ps_prd_n1 ON product_sales (product_id)
/
CREATE INDEX ps_date_n1 ON product_sales (sales_date)
/
BEGIN

  Utils.Clear_Log;
  Bench_Queries.Execute_Run_Batch (
                        p_run_desc              => '&RUNDESC',
                        p_points_wide_2lis      => L2_num_arr (L1_num_arr (100000, 400000, 700000, 1000000)),
                        p_points_deep_2lis      => L2_num_arr (L1_num_arr (2)),
                        p_query_group           => 'DMLSALES',
                        p_redo_data_yn          => 'Y');

END;
/
PROMPT Default log
@../sql/L_Log_Default
PROMPT Execute_Run logs. First batch run: pass 0, then 1, 2... for additional runs
@../sql/L_Log_Gp 0

SELECT 'End: ' || To_Char(SYSDATE,'DD-MON-YYYY HH24:MI:SS') FROM DUAL
/
SPOOL OFF
