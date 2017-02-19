/***************************************************************************************************
Description: Bench_SQL SQL benchmarking framework - test queries across a 2-d dataset space

             Installation script for sys schema to create the bench schema;
             - create directories, pointing to OS directories with read/write access on database 
                server, change the names where necessary: C:\output
             - grant privileges on directories and UTL_File, and select on v_ system tables to bench

             To be run from sys schema before Install_Lib.sql, then Install_Bench.sql, both from bench

Further details: A Framework for Dimensional Benchmarking of SQL Performance 
                 http://aprogrammerwrites.eu/?p=1833

Modification History
Who                  When        Which What
-------------------- ----------- ----- -------------------------------------------------------------
Brendan Furey        05-Nov-2016 1.0   Created
Brendan Furey        18-Feb-2016 1.1   Grants switched from public to bench; added v_$session

***************************************************************************************************/
SET SERVEROUTPUT ON
SET TRIMSPOOL ON
SET PAGES 1000
SET LINES 500

SPOOL ..\out\Install_SYS_v11.log
REM
REM Run this script from sys schema to create new schema for Brendan's benchmarking demo
REM

@..\sql\C_User bench

PROMPT DIRECTORY output_dir - C:\output_v11 *** Change this if necessary, write access required ***
CREATE OR REPLACE DIRECTORY output_dir AS 'C:\output_v11'
/
GRANT WRITE ON DIRECTORY output_dir TO bench
/
GRANT EXECUTE ON UTL_File TO bench
/
GRANT SELECT ON v_$sql TO bench
/
GRANT SELECT ON v_$sql_plan_statistics_all TO bench
/
GRANT SELECT ON v_$mystat TO bench
/
GRANT SELECT ON v_$statname TO bench
/
GRANT SELECT ON v_$sess_time_model TO bench
/
GRANT SELECT ON v_$latch TO bench
/
GRANT SELECT ON v_$database TO bench
/
GRANT SELECT ON v_$version TO bench
/
GRANT SELECT ON v_$session TO bench
/
GRANT EXECUTE ON dbms_xplan_type_table TO bench
/
SPOOL OFF

