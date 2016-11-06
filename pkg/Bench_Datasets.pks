CREATE OR REPLACE PACKAGE Bench_Datasets IS
/***************************************************************************************************
Description: Bench_SQL SQL benchmarking framework - test queries across a 2-d dataset space

             Bench_Datasets package has a fixed spec and for each query group to be benchmarked the
             body procedure must generate test data for the data point passed in.

Further details: A Framework for Dimensional Benchmarking of SQL Performance 
                 http://aprogrammerwrites.eu/?p=1833

Modification History
Who                  When        Which What
-------------------- ----------- ----- -------------------------------------------------------------
Brendan Furey        05-Nov-2016 1.0   Created

***************************************************************************************************/

PROCEDURE Setup_Data (  p_query_group               VARCHAR2, 
                        p_point_wide                PLS_INTEGER, 
                        p_point_deep                PLS_INTEGER,
                        x_num_records           OUT PLS_INTEGER,
                        x_num_records_per_part  OUT NUMBER,
                        x_group_size            OUT NUMBER,
                        x_text                  OUT VARCHAR2);

END Bench_Datasets;
/
SHO ERROR


