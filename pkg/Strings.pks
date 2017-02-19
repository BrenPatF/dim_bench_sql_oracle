PROMPT Types for bracket parsing example, needed for package

DROP TYPE bra_lis_type;
CREATE OR REPLACE TYPE bra_rec_type IS OBJECT (o_pos INTEGER, c_pos INTEGER, str VARCHAR2(4000));
/
CREATE TYPE bra_lis_type IS VARRAY(4000) OF bra_rec_type;
/
CREATE OR REPLACE PACKAGE Strings AS
/***************************************************************************************************
Description: Bench_SQL SQL benchmarking framework - test queries across a 2-d dataset space

             Pipelined functions used in string splitting and bracket parsing examples

Further details: A Framework for Dimensional Benchmarking of SQL Performance 
                 http://aprogrammerwrites.eu/?p=1833

Modification History
Who                  When        Which What
-------------------- ----------- ----- -------------------------------------------------------------
Brendan Furey        05-Feb-2017 1.0   Created

***************************************************************************************************/

FUNCTION Split (p_string VARCHAR2, p_delim VARCHAR2) RETURN L1_chr_db_arr PIPELINED;
FUNCTION Parse_Brackets (p_str VARCHAR2) RETURN bra_lis_type PIPELINED;

END Strings;
/
SHOW ERROR



