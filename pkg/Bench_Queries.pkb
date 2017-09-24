CREATE OR REPLACE PACKAGE BODY Bench_Queries AS
/***************************************************************************************************
Description: SQL benchmarking framework - test queries, DML and DDL across a 2-d dataset space

             Bench_Queries package has:

                Add_Query:      procedure to add a query to a query group
                Create_Run:     procedure to set up the data points and query group for a run
                Execute_Run:    procedures to do the benchmarking for the last run created, or for
                                an id passed in
                Execute_Run_Batch:  procedures to do benchmarking for a batch of data sets 
                Plan_Lines:     function to return the SQL execution plan lines for a marker passed

Further details: A Framework for Dimensional Benchmarking of SQL Performance 
                 http://aprogrammerwrites.eu/?p=1833

Modification History
Who                  When        Which What
-------------------- ----------- ----- -------------------------------------------------------------
Brendan Furey        05-Nov-2016 1.0   Created.
Brendan Furey        03-Dec-2016 1.1   Execute_Run_Batch added; Add_Query: Added p_v12_active_only
Brendan Furey        24-Sep-2017 1.2   Extend to allow DML and DDL benchmarking: Capture pre-query 
                                       counts; rollback after query; post-query SQL
                                       Change random number SQL substitution for timestamp

***************************************************************************************************/

  c_cpu_limit               CONSTANT    NUMBER := 4000000; -- bpf 120113: essentially unlimited
  c_status_f                CONSTANT    VARCHAR2(1) := 'F';
  c_status_s                CONSTANT    VARCHAR2(1) := 'S';
  c_fact_size               CONSTANT    PLS_INTEGER := 15;
  c_flush_threshold         CONSTANT    PLS_INTEGER := 32766;
  c_bulk_limit              CONSTANT    PLS_INTEGER := 1000;
  c_update                  CONSTANT    VARCHAR2(100) := 'UPDATE';
  c_merge                   CONSTANT    VARCHAR2(100) := 'MERGE';
  c_delete                  CONSTANT    VARCHAR2(100) := 'DELETE';
  c_insert                  CONSTANT    VARCHAR2(100) := 'INSERT';
  c_create                  CONSTANT    VARCHAR2(100) := 'CREATE';
  c_ddl                     CONSTANT    VARCHAR2(100) := 'DDL';
  c_dummy_query             CONSTANT    VARCHAR2(100) := 'SELECT ''"1"'' FROM DUAL';

  g_queries                             query_list_type;
  g_query_group                         VARCHAR2(30);
  g_headings                            L1_chr_arr;
  g_points_wide_list                    L1_num_arr;
  g_points_deep_list                    L1_num_arr;
  g_n_tot_recs                          PLS_INTEGER;
  g_file_ptr                            UTL_FILE.File_Type;
  g_buf                                 VARCHAR2(32767);
  g_buf_len                             PLS_INTEGER := 0;
  g_n_write                             PLS_INTEGER;
  g_timer_fil                           PLS_INTEGER;
  g_timer_top                           PLS_INTEGER;

/***************************************************************************************************

Add_Query: Entry point procedure merges a query, with its group into the queries and query_groups
           tables

***************************************************************************************************/
PROCEDURE Add_Query (p_query_name            VARCHAR2,                 -- query name
                     p_query_group           VARCHAR2,                 -- query group
                     p_description           VARCHAR2 DEFAULT NULL,    -- description of query
                     p_group_description     VARCHAR2 DEFAULT NULL,    -- description of group
                     p_active_yn             VARCHAR2 DEFAULT 'Y',     -- active, i.e. includein run?
                     p_text                  CLOB,                     -- query text
                     p_pre_query_sql         CLOB DEFAULT NULL,        -- SQL to run in advance of query
                     p_post_query_sql        CLOB DEFAULT NULL,        -- SQL to run after query
                     p_v12_active_only       BOOLEAN DEFAULT FALSE) IS -- De-activate if Oracle version < 12

  l_active_yn   VARCHAR2(1) := p_active_yn;
  l_ora_vsn     VARCHAR2(10);

BEGIN

  IF p_group_description IS NOT NULL THEN

    MERGE INTO query_groups qgp
    USING (SELECT p_query_group query_group, p_group_description group_description FROM DUAL) par
       ON (qgp.name = par.query_group)
     WHEN MATCHED THEN
       UPDATE SET description = par.group_description
     WHEN NOT MATCHED THEN
       INSERT (
         name,
         description
       ) VALUES (
         par.query_group,
         par.group_description
       );

  END IF;

  IF p_v12_active_only THEN

    SELECT Substr (version, 1, Instr (version, '.', 1, 1) - 1)
      INTO l_ora_vsn
      FROM product_component_version
     WHERE product LIKE 'Oracle Database%';
    Utils.Write_Log ('Oracle major version: ' || l_ora_vsn || ' - making 12c-only query ' || p_query_name || ' inactive for 8, 9, 10, 11...');

    IF l_ora_vsn IN ('8', '9', '10', '11') THEN
      l_active_yn := 'N';
    END IF;

  END IF;

  MERGE INTO queries qry
  USING (SELECT p_query_group query_group, p_query_name query_name, p_description description, l_active_yn active_yn, p_text text, p_pre_query_sql pre_query_sql , p_post_query_sql post_query_sql FROM DUAL) par
     ON (qry.name = par.query_name AND qry.query_group = par.query_group)
   WHEN MATCHED THEN
    UPDATE SET description      = par.description,
               text             = par.text,
               pre_query_sql    = par.pre_query_sql,
               post_query_sql   = par.post_query_sql,
               active_yn        = par.active_yn
   WHEN NOT MATCHED THEN
     INSERT (
       id,
       query_group,
       name,
       description,
       active_yn,
       text,
       pre_query_sql,
       post_query_sql
    ) VALUES (
      queries_s.NEXTVAL,
      p_query_group,
      p_query_name,
      par.description,
      par.active_yn,
      par.text,
      par.pre_query_sql,
      par.post_query_sql
    );

END Add_Query;

/***************************************************************************************************

Init_Statistics: Inserts v$mystat/v$latch/v$sess_time_model 'before' stats iinto bench_run_v$stats
                 before running query for given bench run and data point

***************************************************************************************************/
PROCEDURE Init_Statistics (p_bench_run_statistic_id PLS_INTEGER) AS -- run statistic id
BEGIN

  INSERT INTO bench_run_v$stats (
            bench_run_statistic_id,
            stat_type,
            statistic#,
            level#,
            stat_name,
            value_before,
            wait_before
   )
   WITH unv AS (
          SELECT 'STAT'         stat_type,
                 snm.statistic#,
                 NULL           level#,
                 snm.name       stat_name,
                 stt.value      stat_val,
                 NULL           wait_time
            FROM v$statname     snm
            JOIN v$mystat       stt
              ON stt.statistic# = snm.statistic#
           UNION ALL
          SELECT 'LATCH',
                 latch#,
                 level#,
                     name,
                 gets,
                 wait_time
            FROM v$latch
           UNION ALL
          SELECT 'TIME',
                 stat_id,
                 NULL,
                 stat_name,
                 value,
                 NULL
            FROM v$sess_time_model
           WHERE sid = Sys_Context ('userenv','sid'))
   SELECT   p_bench_run_statistic_id,
            unv.stat_type,
            unv.statistic#,
            unv.level#,
            unv.stat_name,
            unv.stat_val,
            unv.wait_time
     FROM unv;
END Init_Statistics;

/***************************************************************************************************

Plan_Lines: Returns the execution plan into an array, using DBMS_XPlan.Display_Cursor

***************************************************************************************************/
FUNCTION Plan_Lines  (p_sql_marker      VARCHAR2)        -- marker used to identify query in v$
                      RETURN            L1_chr_db_arr IS -- execution plan array

  l_sql_id      VARCHAR2(60) := Utils.Get_SQL_Id (p_sql_marker);
  l_ret_array   L1_chr_db_arr;
BEGIN

  SELECT plan_table_output
    BULK COLLECT INTO l_ret_array
    FROM TABLE (DBMS_XPlan.Display_Cursor (l_sql_id, NULL, 'ALLSTATS LAST')
               );
  RETURN l_ret_array;

END Plan_Lines;

/***************************************************************************************************

Write_Plan_Statistics: Merges v$mystat/v$latch/v$sess_time_model 'after' stats into
                       bench_run_v$stats after running query for given bench run and data point;
                       - inserts v$sql_plan_statistics_all into bench_run_v$sql_plan_stats_all for
                       the query identified from v$sql;
                       - updates bench_run_statistics with the execution plan lines and hash

***************************************************************************************************/
PROCEDURE Write_Plan_Statistics (p_sql_marker                   VARCHAR2,       -- marker used to identify query in v$
                                 p_bench_run_statistic_id       PLS_INTEGER) AS -- run statistic id

  l_sql_id              VARCHAR2(60);
  l_plan_hash_value     NUMBER;
BEGIN

   MERGE INTO bench_run_v$stats stt
   USING (SELECT 'STAT'         stat_type,
                     snm.statistic#,
                 NULL           level#,
                 snm.name       stat_name,
                 stt.value      stat_val,
                 NULL           wait_time
            FROM v$statname     snm
            JOIN v$mystat       stt
              ON stt.statistic# = snm.statistic#
           UNION ALL
          SELECT 'LATCH',
                 latch#,
                 level#,
                 name,
                 gets,
                 wait_time
            FROM v$latch
           UNION ALL
          SELECT 'TIME',
                 stat_id,
                 NULL,
                 stat_name,
                 value,
                 NULL
            FROM v$sess_time_model
           WHERE sid = Sys_Context ('userenv','sid')) unv
       ON (stt.bench_run_statistic_id = p_bench_run_statistic_id
      AND  stt.stat_type              = unv.stat_type
      AND  stt.statistic#             = unv.statistic#
          )
     WHEN MATCHED THEN
       UPDATE SET stt.value_after = unv.stat_val,
                  stt.wait_after = unv.wait_time
     WHEN NOT MATCHED THEN
       INSERT (
            bench_run_statistic_id,
            stat_type,
            statistic#,
            level#,
            stat_name,
            value_after,
            wait_after
       ) VALUES (
            p_bench_run_statistic_id,
            unv.stat_type,
            unv.statistic#,
            unv.level#,
            unv.stat_name,
            unv.stat_val,
            unv.wait_time
       );

  SELECT Max (sql_id) KEEP (DENSE_RANK LAST ORDER BY last_load_time)
    INTO l_sql_id
    FROM v$sql
   WHERE sql_text LIKE '%' || p_sql_marker || '%';

  INSERT INTO bench_run_v$sql_plan_stats_all (
                bench_run_statistic_id,
                id,
                address,
                hash_value,
                sql_id,
                plan_hash_value,
                child_address,
                child_number,
                timestamp,
                operation,
                options,
                object_node,
                object#,
                object_owner,
                object_name,
                object_alias,
                object_type,
                optimizer,
                parent_id,
                depth,
                position,
                search_columns,
                cost,
                cardinality,
                bytes,
                other_tag,
                partition_start,
                partition_stop,
                partition_id,
                other,
                distribution,
                cpu_cost,
                io_cost,
                temp_space,
                access_predicates,
                filter_predicates,
                projection,
                time,
                qblock_name,
                remarks,
                other_xml,
                executions,
                last_starts,
                starts,
                last_output_rows,
                output_rows,
                last_cr_buffer_gets,
                cr_buffer_gets,
                last_cu_buffer_gets,
                cu_buffer_gets,
                last_disk_reads,
                disk_reads,
                last_disk_writes,
                disk_writes,
                last_elapsed_time,
                elapsed_time,
                policy,
                estimated_optimal_size,
                estimated_onepass_size,
                last_memory_used,
                last_execution,
                last_degree,
                total_executions,
                optimal_executions,
                onepass_executions,
                multipasses_executions,
                active_time,
                max_tempseg_size,
                last_tempseg_size)
    SELECT
                p_bench_run_statistic_id,
                id,
                address,
                hash_value,
                sql_id,
                plan_hash_value,
                child_address,
                child_number,
                timestamp,
                operation,
                options,
                object_node,
                object#,
                object_owner,
                object_name,
                object_alias,
                object_type,
                optimizer,
                parent_id,
                depth,
                position,
                search_columns,
                cost,
                cardinality,
                bytes,
                other_tag,
                partition_start,
                partition_stop,
                partition_id,
                other,
                distribution,
                cpu_cost,
                io_cost,
                temp_space,
                access_predicates,
                filter_predicates,
                projection,
                time,
                qblock_name,
                remarks,
                other_xml,
                executions,
                last_starts,
                starts,
                last_output_rows,
                output_rows,
                last_cr_buffer_gets,
                cr_buffer_gets,
                last_cu_buffer_gets,
                cu_buffer_gets,
                last_disk_reads,
                disk_reads,
                last_disk_writes,
                disk_writes,
                last_elapsed_time,
                elapsed_time,
                policy,
                estimated_optimal_size,
                estimated_onepass_size,
                last_memory_used,
                last_execution,
                last_degree,
                total_executions,
                optimal_executions,
                onepass_executions,
                multipasses_executions,
                active_time,
                max_tempseg_size,
                last_tempseg_size
    FROM v$sql_plan_statistics_all
   WHERE sql_id = l_sql_id;

  SELECT Max (plan_hash_value) INTO l_plan_hash_value
    FROM bench_run_v$sql_plan_stats_all
   WHERE bench_run_statistic_id = p_bench_run_statistic_id;

  UPDATE bench_run_statistics
    SET plan_hash_value = l_plan_hash_value,
        plan_tab = Bench_Queries.Plan_Lines (p_sql_marker)
   WHERE id = p_bench_run_statistic_id;

END Write_Plan_Statistics;

/***************************************************************************************************

Get_Queries: Gets the active queries for the group into a global array;
             - parses the query text using regex code and converts it to the form run, with
             statistics hint and in csv format;
             - adds placeholder to be replaced by random number at execution time;
             - hints are preserved

             Parser assumes:
             - select list has one line per expression with a mandatory alias, which can be simple
             or in double-quotes;
             - list has a line with SEL in comment delimiters before and after the list

***************************************************************************************************/
PROCEDURE Get_Queries (p_query_group VARCHAR2) IS -- query group

  CURSOR c_qry IS
  SELECT query_type (id, name, text, pre_query_sql, post_query_sql)
    FROM queries
   WHERE query_group = p_query_group
     AND active_yn   = 'Y'
   ORDER BY order_seq;

  l_mid                 CLOB;
  l_new                 CLOB;
  l_pos_list_beg        PLS_INTEGER;
  l_pos_list_end        PLS_INTEGER;
  l_hint_beg            PLS_INTEGER;
  l_hint_end            PLS_INTEGER;
  l_str                 CLOB;
  l_hint                VARCHAR2(4000);
  l_dml_str             VARCHAR2(100);

BEGIN

  OPEN c_qry;
  FETCH c_qry BULK COLLECT INTO g_queries;
  CLOSE c_qry;

  g_headings := L1_chr_arr ();
  g_headings.Extend (g_queries.COUNT);

  FOR i IN 1..g_queries.COUNT LOOP

    l_hint := '/*+ GATHER_PLAN_STATISTICS ';
    l_str := g_queries(i).text;

    IF l_str IN (c_update, c_merge, c_delete, c_insert, c_ddl, c_create) THEN

      l_dml_str := l_str;

      IF l_str = c_ddl THEN
        g_queries(i).text := Replace (c_dummy_query, 'SELECT', 'SELECT ' || l_hint || g_queries(i).name || ' */' );
      ELSE
        g_queries(i).text := c_dummy_query;
      END IF;

      l_str := g_queries(i).pre_query_sql;
      IF Instr (l_str, '/*+') = 0 THEN
        g_queries(i).pre_query_sql := Replace (l_str, l_dml_str, l_dml_str || l_hint || g_queries(i).name || ' */');
      ELSE
        g_queries(i).pre_query_sql := Replace (l_str, '/*+', l_hint || g_queries(i).name || ' ');
      END IF;
      Utils.Write_Log ('pre_query_sql=' || g_queries(i).pre_query_sql);

      CONTINUE;

    END IF;

    l_pos_list_beg := RegExp_Instr (l_str, '/\* SEL \*/', 1, 1) +  9;
    l_pos_list_end := RegExp_Instr (l_str, '/\* SEL \*/', 1, 2) -  1;
    l_mid := Substr (l_str, l_pos_list_beg, l_pos_list_end - l_pos_list_beg + 1);
--
-- Get any hint in main query, add to the stats hint, and remove from the string, to add back later
--
    l_hint_beg := Instr (l_mid, '/*+', 1, 1) + 3;
    IF l_hint_beg > 3 THEN
      l_hint_end := Instr (l_mid, '*/', l_hint_beg, 1) - 1;
      l_hint := l_hint || Substr (l_mid, l_hint_beg, l_hint_end - l_hint_beg + 1);
      l_mid := Substr (l_mid, 1, l_hint_beg - 4) || Substr (l_mid, l_hint_end + 4);
    END IF;
    l_hint := l_hint || '*/';
--
-- Replace the aliases from select-list with the CSV formatting; also, get the aliases into a CSV headings string
--   'm' means look for ',eol' ($) within string as multi-line (not end of string)
--
    IF Instr (l_mid, '"', 1, 1) > 0 THEN
--
-- Aliases in double-quotes
--
      g_headings (i) :=
        RegExp_Replace (                                                          -- expression alias $ -> alias","random"
         RegExp_Replace (                                                          -- expression alias,$ -> alias","
          l_mid,                                                                    -- select-list string
         '\s+.+\s+"(.+)",$', '\1","', 1, 0, 'm'),                                   -- white space+ || any char+ || white space+ || "(non-white space+)" || ,$
        '\s+.+\s+"(.+)"$', '\1","Random"');                                        -- white space+ || "non-white space+" || $

      l_new :=
         RegExp_Replace (                                                         -- alias $ -> || '","random placeholder"'
              RegExp_Replace (                                                         -- expression alias,$ -> expression || '","' ||
           l_mid,                                                                   -- select-list string
          '\s+(.+)\s+".+",$', ' \1 || ''","'' ||', 1, 0, 'm'),                      -- white space+ || (any char+) || white space+ || "non-white space+" || ,$
             '\s+".+"$', ' || ''","#?"''');                                            -- white space+ || "non-white space+" || $

    ELSE
--
-- Aliases simple. 070612: Added +? after ,$
--
      g_headings (i) :=
        RegExp_Replace (                                                          -- expression alias $ -> alias","random"
         RegExp_Replace (                                                          -- expression alias,$ -> alias","
          l_mid,                                                                    -- select-list string
         '\s+.+\s+(\S+),$+?', '\1","', 1, 0, 'm'),                                   -- white space+ || any char+ || white space+ || (non-white space+) || ,$
        '\s+.+\s+(\S+)$', '\1","Random"');                                        -- white space+ || non-white space+ || $

      l_new :=
         RegExp_Replace (                                                         -- alias $ -> || '","random placeholder"'
              RegExp_Replace (                                                         -- expression alias,$ -> expression || '","' ||
           l_mid,                                                                   -- select-list string
          '\s+(.+)\s+\S+,$+?', ' \1 || ''","'' ||', 1, 0, 'm'),                      -- white space+ || (any char+) || white space+ || non-white space+ || ,$
             '\s+\S+$', ' || ''","#?"''');                                            -- white space+ || non-white space+ || $

    END IF;

    g_headings (i) := '"' ||                                                      -- prepend with "
      Replace (                                                                   -- remove CR
       g_headings (i),                                                             -- headings string
      Chr (10), '');                                                              -- remove CR

    Utils.Write_Log ('Header=' || g_headings (i));
    l_new := '''"'' || ' || l_new;                                                -- prepend with '"' ||

-- move hint section up from here

--
-- Rebuild the query, cleaning up tabs and spaces
--
    g_queries(i).text := '/* ' || g_queries(i).name  || ' */' ||            -- prepend with query name as comment
          Replace (                                                                   -- replace first comment delimiter with the hint string
           Replace (                                                                   -- delete second comment delimiter, identified by start of FROM keyword
            RegExp_Replace (                                                            -- strip multiple spaces to single
             Replace (                                                                   -- replace tab with space
          Replace (                                                                   -- replace CR with space
           Substr (l_str, 1, l_pos_list_beg) ||                                        -- string from start up to first comment delimiter ||
           l_new ||                                                                    --  new select list ||
           Substr (l_str, l_pos_list_end),                                             -- rest of string
          Chr(10), ' '),                                                              -- replace CR with space
         '      ', ' '),                                                                 -- replace tab with space
        '  +', ' '),                                                                -- strip multiple spaces to single
       '/* SEL */ F', 'F'),                                                        -- delete second comment delimiter, identified by start of FROM keyword
      '/* SEL */', l_hint);                                                       -- replace first comment delimiter with the hint string

    Utils.Write_Log ('mid=' || l_mid);
    Utils.Write_Log ('new=' || l_new);
    Utils.Write_Log (g_queries(i).text);

  END LOOP;

END Get_Queries;

/***************************************************************************************************

Flush_Buf: Writes the current buffer to the output file and resets it to null

***************************************************************************************************/
PROCEDURE Flush_Buf IS
BEGIN

  IF g_buf_len = 0 THEN RETURN; END IF;

  Timer_Set.Init_Time (g_timer_fil);
  UTL_File.Put_Line (g_file_ptr, Substr (g_buf, 1, g_buf_len - 1));
  g_buf_len := 0;
  g_buf := NULL;
  Timer_Set.Increment_Time (g_timer_fil, 'Lines');

END Flush_Buf;

/***************************************************************************************************

Write_Line: Writes a line to output buffer, flushing to file first if length exceeds limit

***************************************************************************************************/
PROCEDURE Write_Line (p_line VARCHAR2) IS -- line to write
  l_row       VARCHAR2(4000);
  l_row_len   PLS_INTEGER := 0;
BEGIN

  l_row := p_line || Chr(10);
  l_row_len := Length (l_row);
  IF l_row_len + g_buf_len > c_flush_threshold THEN
    Flush_Buf;
  END IF;
  g_buf_len := l_row_len + g_buf_len;
  g_buf := g_buf || l_row;
  g_n_write := g_n_write + 1;

END Write_Line;

/***************************************************************************************************

Open_File: Opens the output file and initialises the buffer

***************************************************************************************************/
PROCEDURE Open_File (p_name VARCHAR2) IS -- file name (no path)
BEGIN

  g_timer_fil := Timer_Set.Construct ('File Writer');
  g_file_ptr := UTL_File.Fopen ('OUTPUT_DIR', p_name || '.csv', 'W', 32767);
  g_n_write := 0;
  g_buf_len := 0;
  g_buf := NULL;

END Open_File;

/***************************************************************************************************

Close_File: Closes the output file and writes out the file timer set

***************************************************************************************************/
PROCEDURE Close_File IS
BEGIN

  Flush_Buf;
  UTL_File.FClose (g_file_ptr);
  Timer_Set.Write_Times (g_timer_fil);

END Close_File;

/***************************************************************************************************

Outbound_Interface: Runs the query in the context of an outbound interface, writing the output lines
                    to file;
                    - main section:
                        - creates cursor timer set object
                        - opens file
                        - calls Process_Cursor to do query and any pre-query SQL
                        - closes file and sets output parameters

***************************************************************************************************/
PROCEDURE Outbound_Interface (
        p_file_name                         VARCHAR2,       -- output FILE name
        p_query                             query_type,     -- query record
        p_headings                          VARCHAR2,       -- headings string
        p_bench_run_statistics_id           PLS_INTEGER,    -- run statistic id
        x_cpu_time                      OUT NUMBER,         -- cpu time used by query
        x_elapsed_time                  OUT NUMBER,         -- elapsed time used by query
        x_num_records                   OUT PLS_INTEGER,    -- number of records output
        x_num_records_pqs               OUT PLS_INTEGER) IS -- number of records processed in pre-query sql

  TYPE row_list_type IS         VARRAY(10000) OF VARCHAR2(4000);
  l_cur                         SYS_REFCURSOR;
  l_row                         VARCHAR2(4000);
  l_row_list                    row_list_type;
  l_is_first                    BOOLEAN := TRUE;
  l_cpu_time                    NUMBER := 0;
  l_elapsed_time                NUMBER := 0;
  l_ela_inc                     NUMBER;
  l_cpu_inc                     NUMBER;
  l_timer_cur                   PLS_INTEGER;
  l_timer_cur_names             L1_chr_arr := L1_chr_arr (
                                  'Pre SQL', 'Open cursor', 'First fetch', 'Remaining fetches', 
                                  'Write to file', 'Write plan', 'Rollback', 'Post SQL');
  l_ts_ms                       VARCHAR2(100);
  l_bench_run_statistics_id     PLS_INTEGER;
  l_timer_stat_rec              Timer_Set.timer_stat_rec;
  l_num_records_pqs             PLS_INTEGER := 0;
  /***************************************************************************************************

  Process_Cursor: Processes the query after any pre-query SQL;
                  - statistics data are set before and updated/inserted after execution
                  - execution plan data are collected after execution
                  - cursor timer accumulates timings for different steps, and writes after execution
                  - cpu and elapsed times are summed for the direct query execution steps only (plus
                  pre-query SQL, if any)

  ***************************************************************************************************/
  PROCEDURE Process_Cursor (p_cur_str           CLOB,        -- cursor string
                            p_pre_query_sql     CLOB,        -- SQL to run before query
                            p_post_query_sql    CLOB,        -- SQL to run after query
                            p_sql_marker        VARCHAR2) IS -- marker for query
  BEGIN

    Utils.Write_Log (p_cur_str);
    Init_Statistics (p_bench_run_statistic_id => p_bench_run_statistics_id);
    Timer_Set.Init_Time (l_timer_cur);

    COMMIT;
    IF p_pre_query_sql IS NOT NULL THEN
      Utils.Write_Log ('Executing pqs: ' || p_pre_query_sql);
      EXECUTE IMMEDIATE p_pre_query_sql;
      l_num_records_pqs := SQL%ROWCOUNT;
      Utils.Write_Log ('Rows processed: ' || SQL%ROWCOUNT);
    END IF;
    Timer_Set.Increment_Time (l_timer_cur, l_timer_cur_names(1));

    OPEN l_cur FOR p_cur_str;
    Timer_Set.Increment_Time (l_timer_cur, l_timer_cur_names(2));

    LOOP

      FETCH l_cur BULK COLLECT
       INTO l_row_list LIMIT c_bulk_limit;

      IF l_is_first THEN
        Timer_Set.Increment_Time (l_timer_cur, l_timer_cur_names(3));
        l_is_first := FALSE;
      ELSE
        Timer_Set.Increment_Time (l_timer_cur, l_timer_cur_names(4));
      END IF;
      EXIT WHEN l_row_list.COUNT = 0;

      FOR i IN 1..l_row_list.COUNT LOOP

        Write_Line (l_row_list(i));

      END LOOP;
      Timer_Set.Increment_Time (l_timer_cur, l_timer_cur_names(5));

    END LOOP;
    Flush_Buf;

    Timer_Set.Increment_Time (l_timer_cur, l_timer_cur_names(5));
    ROLLBACK;
    Timer_Set.Increment_Time (l_timer_cur, l_timer_cur_names(7));
    IF p_post_query_sql IS NOT NULL THEN
      Utils.Write_Log ('Executing pqs: ' || p_post_query_sql);
      EXECUTE IMMEDIATE p_post_query_sql;
      Utils.Write_Log ('Rows processed: ' || SQL%ROWCOUNT);
    END IF;
    Timer_Set.Increment_Time (l_timer_cur, l_timer_cur_names(8));
    Write_Plan_Statistics (p_sql_marker => p_sql_marker, p_bench_run_statistic_id => p_bench_run_statistics_id);
    COMMIT;
    Utils.Write_Plan (p_sql_marker => p_sql_marker);
    CLOSE l_cur;
    Timer_Set.Increment_Time (l_timer_cur, l_timer_cur_names(6));
    Timer_Set.Write_Times (l_timer_cur);

    FOR i IN 1..4 LOOP

      l_timer_stat_rec := Timer_Set.Get_Timer_Stats (p_timer_set_ind => l_timer_cur, p_timer_name => l_timer_cur_names(i));
      l_cpu_time := l_cpu_time + l_timer_stat_rec.cpu_secs;
      l_elapsed_time := l_elapsed_time + l_timer_stat_rec.ela_secs;

    END LOOP;

  END Process_Cursor;

BEGIN
  l_timer_cur := Timer_Set.Construct ('Cursor');

  Utils.g_group_text := p_query.name;

  Open_File (p_file_name);
  Write_Line (p_headings);

  l_ts_ms := To_Char(SYSTIMESTAMP, 'yymmddhh24missff3');

  Process_Cursor (Replace (p_query.text, '#?', l_ts_ms), 
                  Replace (p_query.pre_query_sql, '1=1', l_ts_ms || '=' || l_ts_ms), 
                  Replace (p_query.post_query_sql, '1=1', l_ts_ms || '=' || l_ts_ms), p_query.name);

  Close_File;
  Utils.Write_Log (g_n_write || ' rows written to ' || p_query.name || '.csv');

  x_num_records := g_n_write;
  x_num_records_pqs := l_num_records_pqs;
  x_cpu_time := l_cpu_time;
  x_elapsed_time := l_elapsed_time;

END Outbound_Interface;

/***************************************************************************************************

Write_Size_list: Writes a list of data point sizes, comma-separated

***************************************************************************************************/
PROCEDURE Write_Size_list (p_list L1_num_arr) IS -- list of sizes
  l_list      VARCHAR2(255);
BEGIN

  FOR i IN 1..p_list.COUNT LOOP
    l_list := l_list || ', ' || p_list(i);
  END LOOP;

  Utils.Write_Log (Substr (l_list, 3));

END Write_Size_list;

/***************************************************************************************************

Write_Twice: Writes a csv line both to the currently open output file, and to the log

***************************************************************************************************/
PROCEDURE Write_Twice (p_csv_str VARCHAR2) IS -- csv string
BEGIN

  Write_Line (p_csv_str);
  Utils.Write_CSV_Fields (p_csv_str => p_csv_str, p_fact_size => c_fact_size);

END Write_Twice;

/***************************************************************************************************

Write_Data_Points: Writes to file and log the list of data points with summary statistics for last
                   latest run

***************************************************************************************************/
PROCEDURE Write_Data_Points IS

  CURSOR c_rdp IS
  SELECT  '"' ||
          'Data Point' ||'","' ||
          size_wide ||'","' ||
          size_deep ||'","' ||
          cpu_time ||'","' ||
          elapsed_time ||'","' ||
          num_records ||'","' ||
          num_records_per_part ||'","' ||
          group_size || '"' csv_row
    FROM bench_run_data_points_v
   ORDER BY point_wide, point_deep;

BEGIN

 Utils.Heading ('Data Points');
 Write_Line ('Data Points');
 Write_Twice (
                  '"' ||
                  'Data Point:' ||'","' ||
                  'size_wide' ||'","' ||
                  'size_deep' ||'","' ||
                  'cpu_time' ||'","' ||
                  'elapsed' ||'","' ||
                  'num_recs' ||'","' ||
                  ' per_part' ||'","' ||
                  'group_size' || '"'
               );
  FOR r_rdp IN c_rdp LOOP

    Write_Twice (r_rdp.csv_row);

  END LOOP;

END Write_Data_Points;

/***************************************************************************************************

Write_Distinct_Plans: Writes to log the distinct execution plans for the latest run

***************************************************************************************************/
PROCEDURE Write_Distinct_Plans IS

  CURSOR c_sps IS
  SELECT s.query_name, s.plan_hash_value,
      Max (s.point_wide) KEEP (DENSE_RANK LAST ORDER BY s.point_wide, s.point_deep) point_wide,
      Max (s.point_deep) KEEP (DENSE_RANK LAST ORDER BY s.point_wide, s.point_deep) point_deep,
      Row_Number() OVER (PARTITION BY s.query_name
          ORDER BY Max (s.point_wide) KEEP (DENSE_RANK LAST ORDER BY s.point_wide, s.point_deep),
                   Max (s.point_deep) KEEP (DENSE_RANK LAST ORDER BY s.point_wide, s.point_deep)) rn,
      Count(*) OVER (PARTITION BY query_name) n_plans
    FROM bench_run_statistics_v s
    CROSS JOIN TABLE (s.plan_tab) t
   GROUP BY s.query_name, s.plan_hash_value
   ORDER BY 1, 3, 4;

  CURSOR c_lin (p_query_name VARCHAR2, p_point_wide NUMBER, p_point_deep NUMBER) IS
  SELECT t.COLUMN_VALUE text
    FROM bench_run_statistics_v s
    CROSS JOIN TABLE (s.plan_tab) t
   WHERE s.query_name     = p_query_name
     AND s.point_wide     = p_point_wide
     AND s.point_deep     = p_point_deep;

BEGIN

  Utils.Heading ('Distinct Plans');
  FOR r_sps IN c_sps LOOP

    Utils.Write_Log (r_sps.query_name || ': ' || r_sps.point_wide || '/' || r_sps.point_deep ||
      ' (' || r_sps.rn || ' of ' || r_sps.n_plans || ')');

    FOR r_lin IN c_lin (r_sps.query_name, r_sps.point_wide, r_sps.point_deep) LOOP
      Utils.Write_Log (r_lin.text);
    END LOOP;

  END LOOP;

END Write_Distinct_Plans;

/***************************************************************************************************

Write_Rows: Write out the fact rows for a given fact using input header and query strings

***************************************************************************************************/
PROCEDURE Write_Rows (p_hdr_str         VARCHAR2,                 -- header string
                      p_qry_str         VARCHAR2,                 -- query string
                      p_fact_name       VARCHAR2,                 -- fact name
                      p_ind_extreme     PLS_INTEGER,              -- last dimension
                      p_ratio           VARCHAR2 DEFAULT NULL) IS -- _RATIO label for headings or null

  l_cur                       SYS_REFCURSOR;
  l_row                       VARCHAR2(500);
  l_row_list                  L1_chr_arr := L1_chr_arr();
  i                           PLS_INTEGER := 0;
  l_qry_local                 VARCHAR2(4000) := Replace (p_qry_str, 'FACT', p_fact_name);

BEGIN

  Utils.Heading (p_fact_name || p_ratio);
  Write_Line ('"' || p_fact_name || p_ratio || '"');
  Write_Twice (p_hdr_str);
  IF p_ratio IS NOT NULL THEN
    l_qry_local := Replace (l_qry_local, 'N f_real', 'N f_ratio');
  END IF;

  OPEN l_cur FOR l_qry_local;
  LOOP
    i := i + 1;
    FETCH l_cur
     INTO l_row;
    EXIT WHEN l_cur%NOTFOUND;
    Write_Twice (l_row);

    IF Mod (i, p_ind_extreme) = 0 THEN
      l_row_list.EXTEND;
      l_row_list (l_row_list.COUNT) := l_row;
    END IF;

  END LOOP;
  CLOSE l_cur;

  Utils.Heading (p_fact_name || '_SLICE' || p_ratio);
  Write_Line ('"' || p_fact_name || '_SLICE' || p_ratio || '"');
  Write_Twice (RegExp_Replace (p_hdr_str, ',"\w*"', '', 1, 1));

  FOR i IN 1..l_row_list.COUNT LOOP

    Write_Twice (RegExp_Replace (l_row_list (i), ',"\w*"', '', 1, 1));

  END LOOP;

  IF p_ratio IS NULL THEN
    Write_Rows (p_hdr_str => p_hdr_str, p_qry_str => p_qry_str, p_fact_name => p_fact_name, p_ind_extreme => p_ind_extreme, p_ratio => '_RATIO');
  END IF;

END Write_Rows;

/***************************************************************************************************

Write_Stat: Writes a statistic, calling Write_Rows after updating input query string

***************************************************************************************************/
PROCEDURE Write_Stat (p_hdr_str         VARCHAR2,       -- header string
                      p_qry_str         VARCHAR2,       -- query string
                      p_stat_type       VARCHAR2,       -- statistic type
                      p_stat_name       VARCHAR2,       -- statistic name
                      p_list_len        PLS_INTEGER) IS -- list length

  l_qry_str VARCHAR2(4000);
BEGIN

  l_qry_str := Replace (Replace (p_qry_str, 'FACT', 'stat_val'), 'bench_v$sql_plan_stats_all_v',
    'bench_run_v$stats_v WHERE stat_name = ''' || p_stat_name || ''' AND stat_type = ''' || p_stat_type || '''');
  Utils.Write_Log (l_qry_str);
  Write_Rows (p_hdr_str, l_qry_str, p_stat_name, p_list_len);

END Write_Stat;

/***************************************************************************************************

Write_All_Facts: Writes distinct plans, data point and plan and fact statistics to file;
                 - fact statistics in either WxD or DxW format according to query string passed
                 - calls Write_Distinct_Plans to write to log the distinct plans
                 - calls Write_Data_Points to write to file and log the data point summary statistics
                 - calls Write_Rows to write to file and log the plan statistics
                 - calls Write_Stat to write to file and log the non-plan statistics

***************************************************************************************************/
PROCEDURE Write_All_Facts (p_hdr_str    VARCHAR2,       -- header string
                           p_qry_str    VARCHAR2,       -- query string
                           p_list_len   PLS_INTEGER) IS -- list length

  CURSOR c_sts IS
  WITH ext AS (
  SELECT  stat_type,
          stat_name,
          Min (stat_val)  min_val,
          Max (stat_val)  max_val
    FROM bench_run_v$stats_v
   WHERE point_wide  = (SELECT Max (point_wide) FROM bench_run_v$stats_v)
     AND point_deep  = (SELECT Max (point_deep) FROM bench_run_v$stats_v)
     AND stat_type   IN ('LATCH', 'STAT')
   GROUP BY stat_type,
            stat_name
  HAVING Max (stat_val) > Greatest (1.1 * Min (stat_val), 500)
  ), rns AS (
  SELECT stat_type,
         Min (stat_name) stat_name,
         min_val,
         max_val,
         Row_Number () OVER (ORDER BY max_val - min_val DESC) diff_rn,
         Row_Number () OVER (ORDER BY max_val / (CASE WHEN min_val = 0 THEN 1 ELSE min_val END) DESC) ratio_rn
    FROM ext
   GROUP BY stat_type, min_val, max_val
  )
  SELECT stat_type,
         stat_name,
         min_val,
         max_val,
         diff_rn,
         ratio_rn
    FROM rns
   WHERE diff_rn <= 5
      OR (ratio_rn <= 5 AND min_val > 0);

  l_fact_name_lis     L1_chr_arr := L1_chr_arr(
      'num_records_out',
      'num_records_pqs',
      'cpu_time',
      'elapsed_time',
      'memory_used',
      'buffers',
      'disk_reads',
      'disk_writes',
      'tempseg_size',
      'cardinality',
      'output_rows',
      'cardinality_error');

BEGIN

  Write_Distinct_Plans;
  Write_Data_Points;

  FOR i IN 1..l_fact_name_lis.COUNT LOOP

    Write_Rows (p_hdr_str, p_qry_str, l_fact_name_lis(i), p_list_len);

  END LOOP;

  Write_Stat (p_hdr_str, p_qry_str, 'STAT', 'sorts (rows)', p_list_len);
  Utils.Heading ('Top Stats');

  FOR r_sts IN c_sts LOOP

    Write_Stat (p_hdr_str, p_qry_str, r_sts.stat_type, r_sts.stat_name, p_list_len);

  END LOOP;

  Close_File;

END Write_All_Facts;

/***************************************************************************************************

Write_Stats: Writes the statistics for a given bench run in csv format;
             - opens the _W and _D files for the given bench run
             - _W has the facts with width as row and depth as column
             - _D other way round
             - calls Write_All_Facts in each case, passing appropriate header and query string

***************************************************************************************************/
PROCEDURE Write_Stats (p_bench_run_id PLS_INTEGER) IS -- bench run id

  l_wide_list                   L1_num_arr;
  l_deep_list                   L1_num_arr;
  l_hdr_str                     VARCHAR2(200);
  l_qry_str                     VARCHAR2(4000);
  l_qry_str_base                VARCHAR2(4000);
  l_qry_str_w                   VARCHAR2(4000) :=
    'WITH wit AS (SELECT query_name, point_wide, size_wide, point_deep, FACT f_real, ' ||
    'Round (FACT / Greatest (Min (FACT) OVER (PARTITION BY point_deep, point_wide), 0.000001), 2) f_ratio FROM bench_v$sql_plan_stats_all_v) ' ||
    'SELECT text FROM (SELECT query_name, point_wide, ''"'' || query_name || ''","W'' || size_wide'; -- from point bpf 311212
  l_qry_str_d                   VARCHAR2(4000) :=
    'WITH wit AS (SELECT query_name, point_deep, size_deep, point_wide, FACT f_real, ' ||
    'Round (FACT / Greatest (Min (FACT) OVER (PARTITION BY point_deep, point_wide), 0.000001), 2) f_ratio FROM bench_v$sql_plan_stats_all_v) ' ||
    'SELECT text FROM (SELECT query_name, point_deep, ''"'' || query_name || ''","D'' || size_deep';
  l_pivot_clause                VARCHAR2(100) := ' || ''","'' || Max (CASE point_deep WHEN DIM_VAL THEN f_real END)';

BEGIN

  Open_File (p_bench_run_id || '_W');

  l_hdr_str := '"Run Type","Width"';
  l_qry_str := l_qry_str_w;
  FOR i IN 1..g_points_deep_list.COUNT LOOP

    l_hdr_str := l_hdr_str || ',"D' || g_points_deep_list (i) || '"';
    l_qry_str := l_qry_str || Replace (l_pivot_clause, 'DIM_VAL', i);

  END LOOP;
  l_qry_str := l_qry_str || ' || ''"'' text FROM wit GROUP BY query_name, point_wide, size_wide) ORDER BY query_name, point_wide';
  Write_All_Facts (p_hdr_str => l_hdr_str, p_qry_str => l_qry_str, p_list_len => g_points_wide_list.COUNT);

  Open_File (p_bench_run_id || '_D');

  l_hdr_str := '"Run Type","Depth"';
  l_qry_str := l_qry_str_d;
  l_pivot_clause := Replace (l_pivot_clause, 'point_deep', 'point_wide');
  FOR i IN 1..g_points_wide_list.COUNT LOOP

    l_hdr_str := l_hdr_str || ',"W' || g_points_wide_list (i) || '"';
    l_qry_str := l_qry_str || Replace (l_pivot_clause, 'DIM_VAL', i);

  END LOOP;
  l_qry_str := l_qry_str || ' || ''"'' text FROM wit GROUP BY query_name, point_deep, size_deep) ORDER BY query_name, point_deep';
  Utils.Write_Log (l_qry_str);
  Write_All_Facts (p_hdr_str => l_hdr_str, p_qry_str => l_qry_str, p_list_len => g_points_deep_list.COUNT);

END Write_Stats;

/***************************************************************************************************

Term_Run: Terminates a run, writing out the top timer and updating the runs table

***************************************************************************************************/
FUNCTION Term_Run (p_bench_run_id       PLS_INTEGER,                 -- bench run id
                   p_status             VARCHAR2 DEFAULT c_status_s, -- status (success/fail)
                   p_message            VARCHAR2 DEFAULT NULL)       -- message
                   RETURN               VARCHAR2 IS                  -- return message

  l_timer_stat_rec              Timer_Set.timer_stat_rec;
BEGIN

  Utils.g_group_text := 'Overall';
  Timer_Set.Write_Times (g_timer_top);

  l_timer_stat_rec := Timer_Set.Get_Timer_Stats (p_timer_set_ind => g_timer_top, p_timer_name => 'Querying');

  UPDATE bench_runs
     SET        cpu_time        = l_timer_stat_rec.cpu_secs,
                elapsed_time    = l_timer_stat_rec.ela_secs,
                status          = p_status,
                message         = p_message
   WHERE id = p_bench_run_id;
  COMMIT;

  IF p_status = c_status_s THEN
    RETURN 'Successfully completed';
  ELSE
    RETURN 'Completed with error: ' || p_message;
  END IF;

END Term_Run;

/***************************************************************************************************

Get_Run_Details: Gets query information and the data points for the run

***************************************************************************************************/
PROCEDURE Get_Run_Details (p_bench_run_id       PLS_INTEGER, -- bench run id
                           x_redo_data_yn   OUT VARCHAR2) IS -- re-create test data flag

  l_log_header_id               PLS_INTEGER;
BEGIN

  g_timer_top := Timer_Set.Construct ('Top');

  SELECT
        query_group,
        log_header_id,
        redo_data_yn
    INTO
        g_query_group,
        l_log_header_id,
        x_redo_data_yn
    FROM (
  SELECT Row_Number() OVER (ORDER BY creation_date) rn,
        id,
        query_group,
        log_header_id,
        redo_data_yn
    FROM bench_runs
   WHERE status = 'I'
     AND id = p_bench_run_id
  ) WHERE rn = 1;

  IF l_log_header_id IS NULL THEN

    l_log_header_id := Utils.Create_Log ('Bench run for ' || g_query_group);

    UPDATE bench_runs
       SET log_header_id = l_log_header_id
     WHERE id = p_bench_run_id;

  END IF;

  Utils.Write_Log ('Bench run id = ' || p_bench_run_id);

  SELECT w.COLUMN_VALUE
    BULK COLLECT INTO g_points_wide_list
    FROM bench_runs r
   CROSS JOIN TABLE (r.points_wide_list) w
   WHERE r.id = p_bench_run_id;

  SELECT d.COLUMN_VALUE
    BULK COLLECT INTO g_points_deep_list
    FROM bench_runs r
   CROSS JOIN TABLE (r.points_deep_list) d
   WHERE r.id = p_bench_run_id;

  Utils.Heading ('Wide Points');
  Write_Size_list (g_points_wide_list);
  Utils.Heading ('Deep Points');
  Write_Size_list (g_points_deep_list);

  Get_Queries (p_query_group => g_query_group);

END Get_Run_Details;

/***************************************************************************************************

Set_Data_Point: Runs the procedure to set up test data, and creates the data point

***************************************************************************************************/
FUNCTION Set_Data_Point (p_bench_run_id         PLS_INTEGER,   -- bench run id
                         p_ind_wide             PLS_INTEGER,   -- data point wide index
                         p_ind_deep             PLS_INTEGER,   -- data point deep index
                         p_redo_data_yn         VARCHAR2)      -- re-create test data flag
                         RETURN                 PLS_INTEGER IS -- bench run data point id

  l_bench_run_data_point_id     PLS_INTEGER;
  l_num_records                 PLS_INTEGER;
  l_num_records_per_part        PLS_INTEGER;
  l_group_size                  PLS_INTEGER;
  l_text                        VARCHAR2(4000);
  l_systimestamp                TIMESTAMP;
  l_cpu_time                    PLS_INTEGER;
  l_point_wide                  PLS_INTEGER := g_points_wide_list(p_ind_wide);
  l_point_deep                  PLS_INTEGER := g_points_deep_list(p_ind_deep);

BEGIN
  Timer_Set.Init_Time (g_timer_top);
  IF p_redo_data_yn = 'Y' THEN

    l_cpu_time := DBMS_Utility.Get_CPU_Time;
    l_systimestamp := SYSTIMESTAMP;

    Bench_Datasets.Setup_Data (
        p_query_group          => g_query_group,
        p_point_wide           => l_point_wide,
        p_point_deep           => l_point_deep,
        x_num_records          => l_num_records,
        x_num_records_per_part => l_num_records_per_part,
        x_group_size           => l_group_size,
        x_text                 => l_text);

    INSERT INTO bench_run_data_points (
        id,
        bench_run_id,
        point_wide,
        point_deep,
        cpu_time,
        elapsed_time,
        num_records,
        num_records_per_part,
        group_size,
        creation_date,
        text
    ) VALUES (
        bench_run_data_points_s.NEXTVAL,
        p_bench_run_id,
        p_ind_wide,
        p_ind_deep,
        (DBMS_Utility.Get_CPU_Time - l_cpu_time)*0.01,
        Utils.Get_Seconds (SYSTIMESTAMP - l_systimestamp),
        l_num_records,
        l_num_records_per_part,
        l_group_size,
        SYSDATE,
        l_text
    ) RETURNING id INTO l_bench_run_data_point_id;

  ELSE

    SELECT Max (id) INTO l_bench_run_data_point_id
      FROM bench_run_data_points;

  END IF;
  Timer_Set.Increment_Time (g_timer_top, 'Setup Data');
  RETURN l_bench_run_data_point_id;

END Set_Data_Point;

/***************************************************************************************************

Run_One: Runs one query against one data point, creating the statistic, then calling Outbound_Interface

***************************************************************************************************/
FUNCTION Run_One (p_query_ind                   PLS_INTEGER, -- query index within group
                  p_bench_run_data_point_id     PLS_INTEGER, -- bench run data point id
                  p_point_wide                  PLS_INTEGER, -- data point wide
                  p_point_deep                  PLS_INTEGER) -- data point deep
                  RETURN                        NUMBER IS    -- cpu time taken

  l_timer                       PLS_INTEGER;
  l_status                      VARCHAR2(1);
  l_message                     VARCHAR2(4000);
  l_num_records                 PLS_INTEGER;
  l_num_records_pqs             PLS_INTEGER;
  l_bench_run_statistics_id     PLS_INTEGER;
  l_cpu_time                    NUMBER;
  l_elapsed_time                NUMBER;

BEGIN

  l_timer := Timer_Set.Construct ('Run_One');

  Utils.g_group_text := g_queries (p_query_ind).name || '-' || p_point_wide || '-' || p_point_deep;

  BEGIN

    l_status := c_status_s;
    INSERT INTO bench_run_statistics (
        id,
        bench_run_data_point_id,
        query_id,
        creation_date,
        status
    ) VALUES (
        bench_run_statistics_s.NEXTVAL,
        p_bench_run_data_point_id,
        g_queries (p_query_ind).id,
        SYSDATE,
        l_status
    ) RETURNING id INTO l_bench_run_statistics_id;

    Outbound_Interface (    p_file_name                 => g_query_group || '_' || g_queries (p_query_ind).name || '_' ||  p_point_wide  || '-' || p_point_deep,
                            p_query                     => g_queries (p_query_ind),
                            p_headings                  => g_headings (p_query_ind),
                            p_bench_run_statistics_id   => l_bench_run_statistics_id,
                            x_cpu_time                  => l_cpu_time,
                            x_elapsed_time              => l_elapsed_time,
                            x_num_records               => l_num_records,
                            x_num_records_pqs           => l_num_records_pqs);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      l_status := c_status_f;
      l_message := SQLERRM;
      Utils.Write_Other_Error (p_package => $$PLSQL_UNIT, p_proc => 'Run_One');
      Close_File;
  END;
  Timer_Set.Increment_Time (l_timer, 'Run');

  Utils.Write_Log ('Summary for W/D = ' || p_point_wide || '/' || p_point_deep || ' , bench_run_statistics_id = ' || l_bench_run_statistics_id);
  Timer_Set.Write_Times (l_timer);

  UPDATE bench_run_statistics
     SET    cpu_time            = l_cpu_time,
            elapsed_time        = l_elapsed_time,
            num_records_out     = l_num_records - 1, -- ignore header
            num_records_pqs     = l_num_records_pqs,
            status              = l_status,
            message             = l_message
   WHERE id = l_bench_run_statistics_id;

  COMMIT;
  RETURN l_cpu_time;

END Run_One;

/***************************************************************************************************

Create_Run: Entry point procedure creates a bench run for given parameters; called before the main
            procedure

***************************************************************************************************/
PROCEDURE Create_Run (  p_run_desc              VARCHAR2,                -- run description
                        p_points_wide_list      L1_num_arr,              -- list of wide data points
                        p_points_deep_list      L1_num_arr,              -- list of deep data points
                        p_query_group           VARCHAR2,                -- query group
                        p_redo_data_yn          VARCHAR2 DEFAULT 'Y') IS -- recreate data set?

  l_bench_run_id        PLS_INTEGER;

BEGIN

  INSERT INTO bench_runs (
        id,
        query_group,
        log_header_id,
        description,
        points_wide_list,
        points_deep_list,
        redo_data_yn,
        status,
        creation_date
  ) VALUES (
        bench_runs_s.NEXTVAL,
        p_query_group,
        NULL, --Utils.Create_Log ('Bench run for ' || p_query_group),
        p_run_desc,
        p_points_wide_list,
        p_points_deep_list,
        p_redo_data_yn,
        'I',
        SYSDATE
  ) RETURNING id INTO l_bench_run_id;
  COMMIT;
  Utils.Write_Log ('Bench run ' || l_bench_run_id || ' created');

END Create_Run;

/***************************************************************************************************

Execute_Run: Entry point procedure to execute the latest bench run created by Create_Run

***************************************************************************************************/
PROCEDURE Execute_Run IS

  l_bench_run_id PLS_INTEGER;

BEGIN

  SELECT Max (id) INTO l_bench_run_id FROM bench_runs;
  Execute_Run (p_bench_run_id => l_bench_run_id);

END Execute_Run;

/***************************************************************************************************

Execute_Run: Performs the benchmarking for a query group across a 2-d range of data points, for a run id;
             usually called by the overloaded parameterless version passing latest run id

***************************************************************************************************/
PROCEDURE Execute_Run (p_bench_run_id PLS_INTEGER) IS -- bench run id

  l_redo_data_yn                VARCHAR2(1);

  l_query_status                L1_num_arr := L1_num_arr (0);
  l_point_wide                  PLS_INTEGER;
  l_point_deep                  PLS_INTEGER;
  l_bench_run_id                PLS_INTEGER;
  l_bench_run_data_point_id     PLS_INTEGER;
  l_n_calls                     PLS_INTEGER;
  k                             PLS_INTEGER;

  Is_Done                     BOOLEAN;


BEGIN

  Get_Run_Details (p_bench_run_id => p_bench_run_id, x_redo_data_yn => l_redo_data_yn);
  FOR i IN 2..g_queries.COUNT LOOP

    l_query_status.EXTEND;
    l_query_status (l_query_status.COUNT):= 0;

  END LOOP;

  FOR i IN 1..g_points_wide_list.COUNT LOOP

    l_point_wide := g_points_wide_list(i);
    FOR k IN 1..g_queries.COUNT LOOP

      IF l_query_status(k) = 1 THEN
        l_query_status(k) := 0;
      END IF;

    END LOOP;

    FOR j IN 1..g_points_deep_list.COUNT LOOP

      l_point_deep := g_points_deep_list(j);
      l_bench_run_data_point_id := Set_Data_Point (p_bench_run_id => p_bench_run_id, p_ind_wide => i, p_ind_deep => j, p_redo_data_yn => l_redo_data_yn);
      FOR l IN  1..g_queries.COUNT LOOP

        IF l_query_status(l) = 0 THEN

          IF Run_One (p_query_ind                     => l,
                      p_bench_run_data_point_id       => l_bench_run_data_point_id,
                      p_point_wide                    => l_point_wide,
                      p_point_deep                    => l_point_deep) > c_cpu_limit THEN

            IF j = 1 THEN
              l_query_status(l) := 2;
            ELSE
              l_query_status(l) := 1;
            END IF;

          END IF;

        END IF;

      END LOOP;
      Timer_Set.Increment_Time (g_timer_top, 'Querying');

      Is_Done := TRUE;
      FOR k IN 1..g_queries.COUNT LOOP

        IF l_query_status(k) != 2 THEN

          Is_Done := FALSE;

        END IF;

      END LOOP;

    END LOOP;

    IF Is_Done THEN
      EXIT;
    END IF;

  END LOOP;
  Write_Stats (p_bench_run_id);
  Utils.Write_Log (Term_Run (l_bench_run_id));

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    Utils.Write_Other_Error (
                p_package       => 'Bench_Queries',
                p_proc          => 'Execute_Run');
    Utils.Write_Log (Term_Run (p_bench_run_id => l_bench_run_id, p_status => c_status_f, p_message => SQLERRM));

END Execute_Run;

/***************************************************************************************************

Execute_Run_Batch: Execute 1 or more runs in a loop, calling Create_Run and Execute_Run

***************************************************************************************************/
PROCEDURE Execute_Run_Batch (p_run_desc              VARCHAR2,                -- run description
                             p_points_wide_2lis      L2_num_arr,              -- list of wide data points, for each run
                             p_points_deep_2lis      L2_num_arr,              -- list of deep data points, for each run
                             p_query_group           VARCHAR2,                -- query group
                             p_redo_data_yn          VARCHAR2 DEFAULT 'Y') IS -- recreate data set?
BEGIN

  IF p_points_wide_2lis.COUNT != p_points_deep_2lis.COUNT THEN

    RAISE_APPLICATION_ERROR (-200001, 'Error wide, deep counts differ (' || p_points_wide_2lis.COUNT || ', ' || p_points_deep_2lis.COUNT || ')');

  END IF;

  Utils.Write_Log ('Executing batch of ' || p_points_wide_2lis.COUNT || ' runs...');
  FOR i IN 1..p_points_wide_2lis.COUNT LOOP

    Create_Run (p_run_desc              => p_run_desc || ' - ' || i,
                p_points_wide_list      => p_points_wide_2lis(i),
                p_points_deep_list      => p_points_deep_2lis(i),
                p_query_group           => p_query_group,
                p_redo_data_yn          => p_redo_data_yn);
    Execute_Run;

  END LOOP;

END Execute_Run_Batch;

END Bench_Queries;
/
SHO ERR


