/***************************************************************************************************
Description: Bench_SQL SQL benchmarking framework - test queries across a 2-d dataset space

             Installation script for the demo examples.
             To be run from bench schema after Install_Lib.sql, and Install_Bench.sql, when bench 
             has been created by Install_SYS.sql

Further details: A Framework for Dimensional Benchmarking of SQL Performance 
                 http://aprogrammerwrites.eu/?p=1833

Example demo scripts:

             Setup_Bur.sql      Sets up the simple bursting example
             Setup_Wts.sql      Sets up the item weights generalised bursting example
             Strings.pks        Package spec for pipelined function for string splitting example
             Strings.pkb        Package body for pipelined function for string splitting example
             Setup_Str.sql      Sets up the string splitting example
             Setup_Bra.sql      Sets up the bracket parsing example
             Bench_Datasets.pkb Specific problem data setup body

Modification History
Who                  When        Which What
-------------------- ----------- ----- -------------------------------------------------------------
Brendan Furey        01-Dec-2016 1.0   Created
Brendan Furey        05-Feb-2017 1.1   Added string splitting and bracket parsing examples
Brendan Furey        05-Mar-2017 1.2   Added Fixed-level hierarchy example

***************************************************************************************************/
SET SERVEROUTPUT ON
SET TRIMSPOOL ON
SET PAGES 1000
SET LINES 500

SPOOL ..\out\Install_Bench_Examples.log

PROMPT Activity simple bursting example
PROMPT ================================
@..\sql\Setup_Bur

PROMPT Item weights generalised bursting example
PROMPT =========================================
@..\sql\Setup_Wts

PROMPT Strings package, used by both bracket parsing and string splitting examples
PROMPT ===========================================================================
@..\pkg\Strings.pks
@..\pkg\Strings.pkb

PROMPT String splitting example
PROMPT ========================
@..\sql\Setup_Str

PROMPT Bracket parsing example
PROMPT =======================
@..\sql\Setup_Bra

PROMPT Fixed-level hierarchy example
PROMPT =============================
@..\sql\Setup_Org

PROMPT Bench_Datasets package body creation
PROMPT ====================================

@..\pkg\Bench_Datasets.pkb

@..\sql\L_Log_Default

SPOOL OFF


