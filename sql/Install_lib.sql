/***************************************************************************************************
Description: Bench_SQL SQL benchmarking framework - test queries across a 2-d dataset space.

             Installation script for bench schema library objects.
             To be run from bench schema before Install_Bench.sql, when bench has been created by
             Install_SYS.sql

Further details: A Framework for Dimensional Benchmarking of SQL Performance 
                 http://aprogrammerwrites.eu/?p=1833

Objects:     
                Tables                          Sequence
                ======                          =========
                log_headers                     log_headers_s
                log_lines                       log_lines_s

                Types           Level 2         Level 3         Comment
                =====           =======         =======         =======
                L1_num_arr      L2_num_arr
                L1_chr_arr      L2_chr_arr      L3_chr_arr      L1_chr_arr is 32,767 
                L1_chr_db_arr                                   4000 for db storage 

                Packages
                =========
                Utils
                Timer_Set

Modification History
Who                  When        Which What
-------------------- ----------- ----- -------------------------------------------------------------
Brendan Furey        05-Nov-2016 1.0   Created
Brendan Furey        30-Nov-2016 1.1   L2_num_arr added
Brendan Furey        01-Dec-2016 1.2   log_headers CASCADE CONSTRAINTS

***************************************************************************************************/
SET SERVEROUTPUT ON
SET TRIMSPOOL ON
SET PAGES 1000
SET LINES 500

SPOOL ..\out\Install_lib.log

REM Run this script from schema for library objects (bench in this case) to create the common objects 

PROMPT Common types creation
PROMPT =====================

PROMPT Drop type L3_chr_arr
DROP TYPE L3_chr_arr
/
PROMPT Drop type L2_chr_arr
DROP TYPE L2_chr_arr
/
PROMPT Create type L1_chr_db_arr
CREATE OR REPLACE TYPE L1_chr_db_arr IS VARRAY(4000) OF VARCHAR2(4000)
/
PROMPT Create type L1_chr_arr
CREATE OR REPLACE TYPE L1_chr_arr IS VARRAY(32767) OF VARCHAR2(32767)
/
PROMPT Create type L2_chr_arr
CREATE OR REPLACE TYPE L2_chr_arr IS VARRAY(32767) OF L1_chr_arr
/
PROMPT Create type L3_chr_arr
CREATE OR REPLACE TYPE L3_chr_arr IS VARRAY(32767) OF L2_chr_arr
/
GRANT EXECUTE ON L1_chr_arr TO PUBLIC
/
GRANT EXECUTE ON L2_chr_arr TO PUBLIC
/
GRANT EXECUTE ON L3_chr_arr TO PUBLIC
/
PROMPT Drop type L2_num_arr
DROP TYPE L2_num_arr
/
PROMPT Create type L1_num_arr
CREATE OR REPLACE TYPE L1_num_arr IS VARRAY(32767) OF NUMBER
/
GRANT EXECUTE ON L1_num_arr TO PUBLIC
/
PROMPT Create type L2_num_arr
CREATE OR REPLACE TYPE L2_num_arr IS VARRAY(32767) OF L1_num_arr
/
GRANT EXECUTE ON L2_num_arr TO PUBLIC
/

PROMPT Common tables creation
PROMPT ======================

PROMPT Create table log_headers
DROP TABLE log_lines
/
DROP TABLE log_headers CASCADE CONSTRAINTS
/
CREATE TABLE log_headers (
        id                      INTEGER NOT NULL,
        description             VARCHAR2(500),
        creation_date           TIMESTAMP,
        CONSTRAINT hdr_pk       PRIMARY KEY (id)
)
/
COMMENT ON TABLE log_headers IS 'Log header, 0-id is for miscellaneous logging'
/
PROMPT Insert the default log header
INSERT INTO log_headers VALUES (0, 'Miscellaneous output', SYSTIMESTAMP)
/
GRANT ALL ON log_headers TO PUBLIC
/
DROP SEQUENCE log_headers_s
/
CREATE SEQUENCE log_headers_s START WITH 1
/
GRANT SELECT ON log_headers_s TO PUBLIC
/
PROMPT Create table log_lines
CREATE TABLE log_lines (
        id                      INTEGER NOT NULL,
        log_header_id           INTEGER NOT NULL,
        group_text              VARCHAR2(100),
        line_text               VARCHAR2(4000),
        creation_date           TIMESTAMP,
        CONSTRAINT lin_pk       PRIMARY KEY (id, log_header_id),
        CONSTRAINT lin_hdr_fk   FOREIGN KEY (log_header_id) REFERENCES log_headers (id)
)
/
COMMENT ON TABLE log_lines IS 'Log lines, linked to header'
/
GRANT ALL ON log_lines TO PUBLIC
/
DROP SEQUENCE log_lines_s
/
CREATE SEQUENCE log_lines_s START WITH 1
/
GRANT SELECT ON log_lines_s TO PUBLIC
/

PROMPT Packages creation
PROMPT =================

PROMPT Create package Utils
@..\pkg\Utils.pks
@..\pkg\Utils.pkb
GRANT EXECUTE ON Utils TO PUBLIC
/

PROMPT Create package Timer_Set
@..\pkg\Timer_Set.pks
@..\pkg\Timer_Set.pkb
GRANT EXECUTE ON Timer_Set TO PUBLIC;

SPOOL OFF


