SET TRIMSPOOL ON
SPOOL ..\out\Batch_Str

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

DEFINE RUNDESC='Str-One'

SET SERVEROUTPUT ON
SET TIMING ON

ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MON-YYYY';
BEGIN

  Utils.Clear_Log;
  Bench_Queries.Execute_Run_Batch (
                        p_run_desc              => '&RUNDESC',
                        p_points_wide_2lis      => L2_num_arr (L1_num_arr (50,100,150,200), 
							                                   L1_num_arr (450,900,1350,1800), 
                                                               L1_num_arr (5), 
                                                               L1_num_arr (150)),
                        p_points_deep_2lis      => L2_num_arr (L1_num_arr (18), 
                                                               L1_num_arr (1), 
                                                               L1_num_arr (195,390,585,780), 
                                                               L1_num_arr (6,12,18,24)),
                        p_query_group           => 'STR_SPLIT',
                        p_redo_data_yn          => 'Y');
/*
  Bench_Queries.Execute_Run_Batch (
                        p_run_desc              => '&RUNDESC',
                        p_points_wide_2lis      => L2_num_arr (L1_num_arr (50,100,150,200), 
                                               L1_num_arr (5)),
                        p_points_deep_2lis      => L2_num_arr (L1_num_arr (18), 
                                               L1_num_arr (195,390,585,780)),
                        p_query_group           => 'STR_SPLIT_RGX',
            p_redo_data_yn      => 'Y');
*/
END;
/
PROMPT Default log
@../sql/L_Log_Default
PROMPT Execute_Run logs
@../sql/L_Log_Gp 3
@../sql/L_Log_Gp 2
@../sql/L_Log_Gp 1
@../sql/L_Log_Gp 0

SELECT 'End: ' || To_Char(SYSDATE,'DD-MON-YYYY HH24:MI:SS') FROM DUAL
/
SPOOL OFF

