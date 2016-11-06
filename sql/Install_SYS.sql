/***************************************************************************************************
Description: Bench_SQL SQL benchmarking framework - test queries across a 2-d dataset space

             Installation script for sys schema to create the bench schema;
             - create directories, pointing to OS directories with read/write access on database 
                server, change the names where necessary: C:\output
             - grant privileges on directories and UTL_File, and select on v_ system tables to public

             To be run from sys schema before Install_Lib.sql, then Install_Bench.sql, both from bench

Further details: A Framework for Dimensional Benchmarking of SQL Performance 
                 http://aprogrammerwrites.eu/?p=1833

Modification History
Who                  When        Which What
-------------------- ----------- ----- -------------------------------------------------------------
Brendan Furey        05-Nov-2016 1.0   Created

***************************************************************************************************/
SET SERVEROUTPUT ON
SET TRIMSPOOL ON
SET PAGES 1000
SET LINES 500

SPOOL ..\out\Install_SYS.log
REM
REM Run this script from sys schema to create new schema for Brendan's benchmarking demo
REM

@..\sql\C_User bench

PROMPT DIRECTORY output_dir - C:\input *** Change this if necessary, write access required ***
CREATE OR REPLACE DIRECTORY output_dir AS 'C:\output'
/
GRANT READ ON DIRECTORY input_dir TO PUBLIC
/
GRANT WRITE ON DIRECTORY output_dir TO PUBLIC
/
GRANT EXECUTE ON UTL_File TO PUBLIC
/
GRANT SELECT ON v_$sql TO PUBLIC
/
GRANT SELECT ON v_$sql_plan_statistics_all TO PUBLIC
/
GRANT SELECT ON v_$mystat TO PUBLIC
/
GRANT SELECT ON v_$statname TO PUBLIC
/
GRANT SELECT ON v_$sess_time_model TO PUBLIC
/
GRANT SELECT ON v_$latch TO PUBLIC
/
GRANT SELECT ON v_$database TO PUBLIC
/
GRANT SELECT ON v_$version TO PUBLIC
/
GRANT EXECUTE ON dbms_xplan_type_table TO PUBLIC
/
SPOOL OFF
