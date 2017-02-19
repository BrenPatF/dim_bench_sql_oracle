SET TRIMSPOOL ON
SPOOL ..\out\Run_Wts

COLUMN "Database"	FORMAT A20
COLUMN "Time"		FORMAT A20
COLUMN "Version"	FORMAT A30
COLUMN "Session"	FORMAT 9999990
COLUMN "OS User"	FORMAT A10
COLUMN "Machine"	FORMAT A20
SET LINES 180
SET PAGES 1000

SELECT 'Start: ' || dbs.name "Database", To_Char (SYSDATE,'DD-MON-YYYY HH24:MI:SS') "Time",
	Replace (Substr(ver.banner, 1, Instr(ver.banner, '64')-4), 'Enterprise Edition Release ', '') "Version"
  FROM v$database dbs,  v$version ver
 WHERE ver.banner LIKE 'Oracle%';

DEFINE RUNDESC='Itm-One'

SET SERVEROUTPUT ON
SET TIMING ON

ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MON-YYYY';
BEGIN

  Utils.Clear_Log;
  Bench_Queries.Create_Run (
			p_run_desc		=> '&RUNDESC',
			p_points_wide_list	=> L1_num_arr (10),
			p_points_deep_list	=> L1_num_arr (500, 1000, 2000),
			p_query_group		=> 'WEIGHTS',
                        p_redo_data_yn          => 'Y');
  Bench_Queries.Execute_Run;

END;
/
PROMPT Default log
@../sql/L_Log_Default
PROMPT Execute_Run log
@../sql/L_Log_Gp 0

SELECT 'End: ' || To_Char(SYSDATE,'DD-MON-YYYY HH24:MI:SS') FROM DUAL
/
SPOOL OFF

