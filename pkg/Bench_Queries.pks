CREATE OR REPLACE PACKAGE Bench_Queries AS
/***************************************************************************************************
Description: Bench_SQL SQL benchmarking framework - test queries across a 2-d dataset space

             Bench_Queries package has:

                Add_Query:      procedure to add a query to a query group
                Create_Run:     procedure to set up the data points and query group for a run
                Execute_Run:    procedures to do the benchmarking for the last run created, or for
                                an id passed in
                Plan_Lines:     function to return the SQL execution plan lines for a marker passed

Further details: A Framework for Dimensional Benchmarking of SQL Performance 
                 http://aprogrammerwrites.eu/?p=1833

Modification History
Who                  When        Which What
-------------------- ----------- ----- -------------------------------------------------------------
Brendan Furey        05-Nov-2016 1.0   Created

***************************************************************************************************/

PROCEDURE Add_Query (p_query_name            VARCHAR2,
                     p_query_group           VARCHAR2,
                     p_description           VARCHAR2 DEFAULT NULL,
                     p_group_description     VARCHAR2 DEFAULT NULL,
                     p_active_yn             VARCHAR2 DEFAULT 'Y',
                     p_text                  CLOB,
                     p_pre_query_sql         CLOB DEFAULT NULL);
PROCEDURE Create_Run (p_run_desc              VARCHAR2,
                      p_points_wide_list      L1_num_arr,
                      p_points_deep_list      L1_num_arr,
                      p_query_group           VARCHAR2,
                      p_redo_data_yn          VARCHAR2 DEFAULT 'Y');
PROCEDURE Execute_Run (p_bench_run_id PLS_INTEGER);
PROCEDURE Execute_Run;
FUNCTION Plan_Lines  (p_sql_marker VARCHAR2) RETURN L1_chr_db_arr;

END Bench_Queries;
/
SHO ERR



