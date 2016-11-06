/***************************************************************************************************
Description: Bench_SQL SQL benchmarking framework - test queries across a 2-d dataset space

             Installation script for main bench schema objects, including the demo objects.
             To be run from bench schema after Install_Lib.sql, when bench has been created by
             Install_SYS.sql

Further details: A Framework for Dimensional Benchmarking of SQL Performance 
                 http://aprogrammerwrites.eu/?p=1833

General bench Objects:

             Tables                          Sequence                Comment
             ======                          =========               =======
             query_groups                                            Query group
             queries                         queries_s               Query and pre-query SQL text
             bench_runs                      bench_runs_s            Bench run header
             bench_run_data_points           bench_run_data_points_s Bench run data points
             bench_run_statistics            bench_run_statistics_s
             bench_run_v$sql_plan_stats_all
             bench_run_v$stats

             Views (all for latest run)
             =====
             bench_run_data_points_v         bench_run_data_points with actual wide and deep sizes
                                             etc.
             bench_run_statistics_v          bench_run_statistics with actual wide and deep sizes
                                             etc.
             bench_v$sql_plan_stats_all_v    bench_run_v$sql_plan_stats_all with actual wide and
                                             deep sizes etc., grouping for maximum values
             bench_run_v$stats_v             bench_run_v$stats with actual wide and deep sizes etc.
             bench_run_dp_statistics_v       bench_run_data_points with actual wide and deep sizes
                                             etc., with bench_run_statistics

             Types
             =====
             query_type                      Query metadata
             query_list_type                 List of query metadata

             Packages
             =========
             Bench_Queries                   Generic dimensional benchmarking
             Bench_Datasets                  Generic spec for specific problem data setup body

Bursting demo scripts:

             ..\sql\Act_Bur.sql              Sets up the bursting data structures and populates 
                                             table with simple functional test data, queries it
             ..\sql\I_Queries.sql            Insert queries for bursting problem
             ..\pkg\Bench_Datasets.pkb       Specific problem data setup body (with generic spec)

Modification History
Who                  When        Which What
-------------------- ----------- ----- -------------------------------------------------------------
Brendan Furey        05-Nov-2016 1.0   Created

***************************************************************************************************/
SET SERVEROUTPUT ON
SET TRIMSPOOL ON
SET PAGES 1000
SET LINES 500

SPOOL ..\out\Install_Bench.log

PROMPT Dropping tables
DROP TABLE bench_run_v$stats
/
DROP TABLE bench_run_v$sql_plan_stats_all
/
DROP TABLE bench_run_statistics
/
DROP TABLE bench_run_data_points
/
DROP TABLE bench_runs
/
DROP TABLE queries
/
DROP TABLE query_groups
/
PROMPT query_groups
CREATE TABLE query_groups (
    name                         VARCHAR2(30) NOT NULL,
    description                  VARCHAR2(500),
    CONSTRAINT qgp_pk            PRIMARY KEY (name))
/
COMMENT ON TABLE query_groups IS 'Query group'
/
PROMPT Queries
CREATE TABLE queries (
    id                           NUMBER NOT NULL,
    query_group                  VARCHAR2(30) NOT NULL,
    name                         VARCHAR2(30) NOT NULL,
    order_seq                    NUMBER,
    description                  VARCHAR2(500),
    active_yn                    VARCHAR2(1) DEFAULT 'Y',
    text                         CLOB NOT NULL,
    pre_query_sql                CLOB,
    CONSTRAINT qry_pk            PRIMARY KEY (id),
    CONSTRAINT qry_uk            UNIQUE (query_group, name),
    CONSTRAINT qry_qgp_fk        FOREIGN KEY (query_group) REFERENCES query_groups (name)
)
/
COMMENT ON TABLE queries IS 'Query and pre-query SQL text'
/
PROMPT queries_s
DROP SEQUENCE queries_s
/
CREATE SEQUENCE queries_s START WITH 1
/
PROMPT bench_runs
CREATE TABLE bench_runs (
    id                           NUMBER NOT NULL,
    description                  VARCHAR2(500),
    query_group                  VARCHAR2(30) NOT NULL,
    log_header_id		 NUMBER,
    data_set                     VARCHAR2(30),
    points_wide_list             L1_num_arr,
    points_deep_list             L1_num_arr,
    redo_data_yn                 VARCHAR2(1) DEFAULT 'Y',
    status                       VARCHAR2(1),
    message                      VARCHAR2(4000),
    cpu_time                     NUMBER,
    elapsed_time                 NUMBER,
    creation_date                DATE NOT NULL,
    CONSTRAINT brn_pk            PRIMARY KEY (id),
    CONSTRAINT brn_qgp_fk        FOREIGN KEY (query_group) REFERENCES query_groups (name),
    CONSTRAINT brn_log_fk        FOREIGN KEY (log_header_id) REFERENCES log_headers (id)
)
/
COMMENT ON TABLE bench_runs IS 'Bench run header'
/
PROMPT bench_runs_s
DROP SEQUENCE bench_runs_s
/
CREATE SEQUENCE bench_runs_s START WITH 1
/
PROMPT bench_run_data_points
CREATE TABLE bench_run_data_points (
    id                              NUMBER NOT NULL,
    bench_run_id                    NUMBER NOT NULL,
    point_wide                      NUMBER NOT NULL,
    point_deep                      NUMBER NOT NULL,
    cpu_time                        NUMBER,
    elapsed_time                    NUMBER,
    num_records                     NUMBER,
    num_records_per_part            NUMBER,
    group_size                      NUMBER,
    creation_date                   DATE NOT NULL,
    text                            VARCHAR2(4000),
    CONSTRAINT rdp_pk               PRIMARY KEY (id),
    CONSTRAINT rdp_uk               UNIQUE (bench_run_id, point_wide, point_deep),
    CONSTRAINT rdp_rcn_fk           FOREIGN KEY (bench_run_id) REFERENCES bench_runs (id)
)
/
COMMENT ON TABLE bench_run_data_points IS 'Bench run data points'
/
PROMPT bench_run_data_points_s
DROP SEQUENCE bench_run_data_points_s
/
CREATE SEQUENCE bench_run_data_points_s START WITH 1
/
PROMPT bench_run_statistics
CREATE TABLE bench_run_statistics (
    id                              NUMBER NOT NULL,
    bench_run_data_point_id         NUMBER NOT NULL,
    query_id                        NUMBER NOT NULL,
    cpu_time                        NUMBER,
    elapsed_time                    NUMBER,
    num_records_out                 NUMBER,
    plan_hash_value                 NUMBER,
    plan_tab                        L1_chr_db_arr,
    creation_date                   DATE NOT NULL,
    status                          VARCHAR2(1),
    message                         VARCHAR2(4000),
    CONSTRAINT brs_pk               PRIMARY KEY (id),
    CONSTRAINT brs_uk               UNIQUE (bench_run_data_point_id, query_id),
    CONSTRAINT brs_qry_fk           FOREIGN KEY (query_id) REFERENCES queries (id),
    CONSTRAINT brs_rdp_fk           FOREIGN KEY (bench_run_data_point_id) REFERENCES bench_run_data_points (id)
)
/
COMMENT ON TABLE bench_run_statistics IS 'Summary timing statistics by run data point'
/
PROMPT bench_run_statistics_s
DROP SEQUENCE bench_run_statistics_s
/
CREATE SEQUENCE bench_run_statistics_s START WITH 1
/
PROMPT bench_run_v$sql_plan_stats_all
CREATE TABLE bench_run_v$sql_plan_stats_all (
        bench_run_statistic_id      NUMBER NOT NULL,
        ID                          NUMBER NOT NULL,
        ADDRESS                     RAW(8),
        HASH_VALUE                  NUMBER,
        SQL_ID                      VARCHAR2(13),
        PLAN_HASH_VALUE             NUMBER,
        CHILD_ADDRESS               RAW(8),
        CHILD_NUMBER                NUMBER,
        TIMESTAMP                   DATE,
        OPERATION                   VARCHAR2(30),
        OPTIONS                     VARCHAR2(30),
        OBJECT_NODE                 VARCHAR2(40),
        OBJECT#                     NUMBER,
        OBJECT_OWNER                VARCHAR2(30),
        OBJECT_NAME                 VARCHAR2(30),
        OBJECT_ALIAS                VARCHAR2(65),
        OBJECT_TYPE                 VARCHAR2(20),
        OPTIMIZER                   VARCHAR2(20),
        PARENT_ID                   NUMBER,
        DEPTH                       NUMBER,
        POSITION                    NUMBER,
        SEARCH_COLUMNS              NUMBER,
        COST                        NUMBER,
        CARDINALITY                 NUMBER,
        BYTES                       NUMBER,
        OTHER_TAG                   VARCHAR2(35),
        PARTITION_START             VARCHAR2(5),
        PARTITION_STOP              VARCHAR2(5),
        PARTITION_ID                NUMBER,
        OTHER                       VARCHAR2(4000),
        DISTRIBUTION                VARCHAR2(20),
        CPU_COST                    NUMBER,
        IO_COST                     NUMBER,
        TEMP_SPACE                  NUMBER,
        ACCESS_PREDICATES           VARCHAR2(4000),
        FILTER_PREDICATES           VARCHAR2(4000),
        PROJECTION                  VARCHAR2(4000),
        TIME                        NUMBER,
        QBLOCK_NAME                 VARCHAR2(30),
        REMARKS                     VARCHAR2(4000),
        OTHER_XML                   CLOB,
        EXECUTIONS                  NUMBER,
        LAST_STARTS                 NUMBER,
        STARTS                      NUMBER,
        LAST_OUTPUT_ROWS            NUMBER,
        OUTPUT_ROWS                 NUMBER,
        LAST_CR_BUFFER_GETS         NUMBER,
        CR_BUFFER_GETS              NUMBER,
        LAST_CU_BUFFER_GETS         NUMBER,
        CU_BUFFER_GETS              NUMBER,
        LAST_DISK_READS             NUMBER,
        DISK_READS                  NUMBER,
        LAST_DISK_WRITES            NUMBER,
        DISK_WRITES                 NUMBER,
        LAST_ELAPSED_TIME           NUMBER,
        ELAPSED_TIME                NUMBER,
        POLICY                      VARCHAR2(10),
        ESTIMATED_OPTIMAL_SIZE      NUMBER,
        ESTIMATED_ONEPASS_SIZE      NUMBER,
        LAST_MEMORY_USED            NUMBER,
        LAST_EXECUTION              VARCHAR2(10),
        LAST_DEGREE                 NUMBER,
        TOTAL_EXECUTIONS            NUMBER,
        OPTIMAL_EXECUTIONS          NUMBER,
        ONEPASS_EXECUTIONS          NUMBER,
        MULTIPASSES_EXECUTIONS      NUMBER,
        ACTIVE_TIME                 NUMBER,
        MAX_TEMPSEG_SIZE            NUMBER,
        LAST_TEMPSEG_SIZE           NUMBER,
--        CONSTRAINT rps_pk           PRIMARY KEY (bench_run_statistic_id, id), I encountered a duplicate
        CONSTRAINT rps_rst_fk       FOREIGN KEY (bench_run_statistic_id) REFERENCES bench_run_statistics (id)
)
/
COMMENT ON TABLE bench_run_v$sql_plan_stats_all IS 'Query execution statistics from v$sql_plan_statistics_all by run statistic id'
/
CREATE INDEX brs_N1 ON bench_run_v$sql_plan_stats_all (bench_run_statistic_id, id) -- was PK
/
CREATE TABLE bench_run_v$stats (
        bench_run_statistic_id      NUMBER NOT NULL,
        stat_type                   VARCHAR2(30),
        statistic#                  NUMBER,
        level#                      NUMBER,
        stat_name                   VARCHAR2(100),
        value_before                NUMBER,
        value_after                 NUMBER,
        wait_before                 NUMBER,
        wait_after                  NUMBER,
        CONSTRAINT rvs_pk           PRIMARY KEY (bench_run_statistic_id, statistic#, stat_type),
        CONSTRAINT rvs_rst_fk       FOREIGN KEY (bench_run_statistic_id) REFERENCES bench_run_statistics (id)
)
/
COMMENT ON TABLE bench_run_v$stats IS 'Before and after query values from v$mystat, v$latch, v$sess_time_model'
/
PROMPT bench_run_data_points_v
CREATE OR REPLACE VIEW bench_run_data_points_v AS
WITH last_run_v AS (
SELECT Max (id) id FROM bench_runs
), wide_v AS (
SELECT r.id, ROWNUM ind, s.COLUMN_VALUE siz
  FROM bench_runs r
  JOIN last_run_v l
    ON l.id = r.id
  CROSS JOIN TABLE (r.points_wide_list) s
), deep_v AS (
SELECT r.id, ROWNUM ind, s.COLUMN_VALUE siz
  FROM bench_runs r
  JOIN last_run_v l
    ON l.id = r.id
  CROSS JOIN TABLE (r.points_deep_list) s
)
SELECT  rdp.point_wide,
        rdp.point_deep,
        wid.siz size_wide,
        dee.siz size_deep,
        rdp.cpu_time,
        rdp.elapsed_time,
        rdp.num_records,
        rdp.num_records_per_part,
        rdp.group_size
  FROM bench_run_data_points rdp
  JOIN last_run_v lrv
    ON lrv.id = rdp.bench_run_id
  JOIN wide_v wid
    ON wid.id = rdp.bench_run_id
   AND wid.ind = rdp.point_wide
  JOIN deep_v dee
    ON dee.id = rdp.bench_run_id
   AND dee.ind = rdp.point_deep
/
PROMPT bench_run_statistics_v
CREATE OR REPLACE VIEW bench_run_statistics_v AS
WITH last_run_v AS (
SELECT Max (id) id FROM bench_runs
), wide_v AS (
SELECT r.id, ROWNUM ind, s.COLUMN_VALUE siz
  FROM bench_runs r
  JOIN last_run_v l
    ON l.id = r.id
  CROSS JOIN TABLE (r.points_wide_list) s
), deep_v AS (
SELECT r.id, ROWNUM ind, s.COLUMN_VALUE siz
  FROM bench_runs r
  JOIN last_run_v l
    ON l.id = r.id
  CROSS JOIN TABLE (r.points_deep_list) s
)
SELECT  qry.name query_name,
        rdp.point_wide,
        rdp.point_deep,
        wid.siz size_wide,
        dee.siz size_deep,
        rdp.num_records num_records_total,
        brs.num_records_out,
        brs.cpu_time,
        brs.elapsed_time,
        brs.plan_hash_value,
        brs.plan_tab,
        To_Char (brs.creation_date, 'dd-Mon-yy hh24:mi:ss') created
  FROM bench_run_data_points rdp
  JOIN last_run_v lrv
    ON lrv.id = rdp.bench_run_id
  JOIN wide_v wid
    ON wid.id = rdp.bench_run_id
   AND wid.ind = rdp.point_wide
  JOIN deep_v dee
    ON dee.id = rdp.bench_run_id
   AND dee.ind = rdp.point_deep
  JOIN bench_run_statistics brs
    ON brs.bench_run_data_point_id = rdp.id
  JOIN queries qry
    ON qry.id = brs.query_id
/
PROMPT bench_v$sql_plan_stats_all_v
CREATE OR REPLACE VIEW bench_v$sql_plan_stats_all_v AS
WITH last_run_v AS (
SELECT Max (id) id FROM bench_runs
), wide_v AS (
SELECT r.id, ROWNUM ind, s.COLUMN_VALUE siz
  FROM bench_runs r
  JOIN last_run_v l
    ON l.id = r.id
  CROSS JOIN TABLE (r.points_wide_list) s
), deep_v AS (
SELECT r.id, ROWNUM ind, s.COLUMN_VALUE siz
  FROM bench_runs r
  JOIN last_run_v l
    ON l.id = r.id
  CROSS JOIN TABLE (r.points_deep_list) s
)
SELECT  rps.bench_run_statistic_id,
        rdp.bench_run_id,
        qry.name query_name,
        rps.hash_value,
        rps.sql_id,
        rps.plan_hash_value,
        rdp.point_wide,
        rdp.point_deep,
        wid.siz size_wide,
        dee.siz size_deep,
        rdp.num_records num_records_total,
        brs.num_records_out,
        brs.cpu_time,
        brs.elapsed_time,
        To_Char (brs.creation_date, 'dd-Mon-yy hh24:mi:ss') created,
        Max (rps.last_tempseg_size)                         tempseg_size,
        Max (rps.last_memory_used)                          memory_used,
        Max (rps.last_cr_buffer_gets + rps.last_cu_buffer_gets) buffers,
        Max (rps.last_disk_reads)                           disk_reads,
        Max (rps.last_disk_writes)                          disk_writes,
        Max (rps.cardinality)                               cardinality,
        Max (rps.last_output_rows)                          output_rows,
        Max (Abs (rps.last_output_rows - rps.last_starts * rps.cardinality))  cardinality_error
  FROM bench_run_v$sql_plan_stats_all rps
  JOIN bench_run_statistics brs
    ON brs.id = rps.bench_run_statistic_id
  JOIN bench_run_data_points rdp
    ON rdp.id = brs.bench_run_data_point_id
  JOIN last_run_v lrv
    ON lrv.id = rdp.bench_run_id
  JOIN wide_v wid
    ON wid.id = rdp.bench_run_id
   AND wid.ind = rdp.point_wide
  JOIN deep_v dee
    ON dee.id = rdp.bench_run_id
   AND dee.ind = rdp.point_deep
  JOIN queries qry
    ON qry.id = brs.query_id
 WHERE rps.bench_run_statistic_id IN (
    SELECT id
      FROM bench_run_statistics
     WHERE bench_run_id = lrv.id
    )
 GROUP BY rps.bench_run_statistic_id,
        rdp.bench_run_id,
        qry.name,
        rps.hash_value,
        rps.sql_id,
        rps.plan_hash_value,
        rdp.point_wide,
        rdp.point_deep,
        wid.siz,
        dee.siz,
        rdp.num_records,
        brs.num_records_out,
        brs.cpu_time,
        brs.elapsed_time,
        To_Char (brs.creation_date, 'dd-Mon-yy hh24:mi:ss')
/
PROMPT bench_run_v$stats_v
CREATE OR REPLACE VIEW bench_run_v$stats_v (
        bench_run_statistic_id,
        query_name,
        point_wide,
        point_deep,
        size_wide,
        size_deep,
        stat_type,
        statistic#,
        level#,
        stat_name,
        stat_val,
        wait_time)
AS
WITH last_run_v AS (
SELECT Max (id) id FROM bench_runs
), wide_v AS (
SELECT r.id, ROWNUM ind, s.COLUMN_VALUE siz
  FROM bench_runs r
  JOIN last_run_v l
    ON l.id = r.id
  CROSS JOIN TABLE (r.points_wide_list) s
), deep_v AS (
SELECT r.id, ROWNUM ind, s.COLUMN_VALUE siz
  FROM bench_runs r
  JOIN last_run_v l
    ON l.id = r.id
  CROSS JOIN TABLE (r.points_deep_list) s
)
SELECT  v$s.bench_run_statistic_id,
        qry.name query_name,
        rdp.point_wide,
        rdp.point_deep,
        wid.siz,
        dee.siz,
        v$s.stat_type,
        v$s.statistic#,
        v$s.level#,
        v$s.stat_name,
        Nvl (v$s.value_after, 0) - Nvl (v$s.value_before, 0),
        Nvl (v$s.wait_after, 0) - Nvl (v$s.wait_before, 0)
  FROM bench_run_v$stats v$s
  JOIN bench_run_statistics brs
    ON brs.id = v$s.bench_run_statistic_id
  JOIN bench_run_data_points rdp
    ON rdp.id = brs.bench_run_data_point_id
  JOIN last_run_v lrv
    ON lrv.id = rdp.bench_run_id
  JOIN wide_v wid
    ON wid.id = rdp.bench_run_id
   AND wid.ind = rdp.point_wide
  JOIN deep_v dee
    ON dee.id = rdp.bench_run_id
   AND dee.ind = rdp.point_deep
  JOIN queries qry
    ON qry.id = brs.query_id
 WHERE v$s.bench_run_statistic_id IN (
    SELECT id
      FROM bench_run_statistics
     WHERE bench_run_id = lrv.id
    )
/
CREATE OR REPLACE VIEW bench_run_dp_statistics_v (
    bench_run_data_point_id,
    bench_run_id,
    point_wide,
    point_deep,
    size_wide,
    size_deep,
    rdp_cpu_time,
    rdp_elapsed_time,
    num_records,
    num_records_per_part,
    group_size,
    rdp_creation_date,
    text,
    bench_run_statistic_id,
    query_id,
    query_name,
    rst_cpu_time,
    rst_elapsed_time,
    num_records_out,
    rst_creation_date,
    status,
    message
) AS
WITH last_run_v AS (
SELECT Max (id) id FROM bench_runs
), wide_v AS (
SELECT r.id, ROWNUM ind, s.COLUMN_VALUE siz
  FROM bench_runs r
  JOIN last_run_v l
    ON l.id = r.id
  CROSS JOIN TABLE (r.points_wide_list) s
), deep_v AS (
SELECT r.id, ROWNUM ind, s.COLUMN_VALUE siz
  FROM bench_runs r
  JOIN last_run_v l
    ON l.id = r.id
  CROSS JOIN TABLE (r.points_deep_list) s
)
SELECT
    rdp.id,
    rdp.bench_run_id,
    rdp.point_wide,
    rdp.point_deep,
    wid.siz,
    dee.siz,
    rdp.cpu_time,
    rdp.elapsed_time,
    rdp.num_records,
    rdp.num_records_per_part,
    rdp.group_size,
    rdp.creation_date,
    rdp.text,
    rst.id,
    rst.query_id,
    qry.name,
    rst.cpu_time,
    rst.elapsed_time,
    rst.num_records_out,
    rst.creation_date,
    rst.status,
    rst.message
  FROM bench_run_data_points rdp
  JOIN last_run_v lrv
    ON lrv.id = rdp.bench_run_id
  JOIN wide_v wid
    ON wid.id = rdp.bench_run_id
   AND wid.ind = rdp.point_wide
  JOIN deep_v dee
    ON dee.id = rdp.bench_run_id
   AND dee.ind = rdp.point_deep
  LEFT JOIN bench_run_statistics rst
    ON bench_run_data_point_id = rdp.id
  LEFT JOIN queries qry
    ON qry.id = rst.query_id
/
DROP TYPE query_list_type
/
CREATE OR REPLACE TYPE query_type AS OBJECT (id INTEGER, name VARCHAR2(30), text CLOB, pre_query_sql CLOB)
/
CREATE OR REPLACE TYPE query_list_type AS VARRAY(100) OF query_type
/
CREATE OR REPLACE CONTEXT bench_ctx USING Bench_Datasets;

PROMPT Packages creation
PROMPT =================

PROMPT Create general packages
@..\pkg\Bench_Datasets.pks
@..\pkg\Bench_Queries.pks
@..\pkg\Bench_Queries.pkb

PROMPT Next scripts are specific to the query group being tested
@..\sql\Act_Bur
@..\sql\I_Queries
@..\pkg\Bench_Datasets.pkb

@..\sql\L_Log_Default

SPOOL OFF
