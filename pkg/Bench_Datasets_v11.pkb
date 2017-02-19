CREATE OR REPLACE PACKAGE BODY Bench_Datasets IS
/***************************************************************************************************
Description: Bench_SQL SQL benchmarking framework - test queries across a 2-d dataset space

             Bench_Datasets package has a fixed spec and for each query group to be benchmarked the
             body procedure must generate test data for the data point passed in.

Further details: A Framework for Dimensional Benchmarking of SQL Performance 
                 http://aprogrammerwrites.eu/?p=1833

Modification History
Who                  When        Which What
-------------------- ----------- ----- -------------------------------------------------------------
Brendan Furey        05-Nov-2016 1.0   Created, with 'BURST'
Brendan Furey        19-Nov-2016 1.1   Added 'WEIGHTS'
Brendan Furey        27-Nov-2016 1.2   Added 'STR_SPLIT'
Brendan Furey        05-Feb-2017 1.3   Added 'BRACKET'

***************************************************************************************************/
  c_query_group_wts        CONSTANT VARCHAR2(30) := 'WEIGHTS';
  c_query_group_bur        CONSTANT VARCHAR2(30) := 'BURST';
  c_query_group_str        CONSTANT VARCHAR2(30) := 'STR_SPLIT';
  c_query_group_bra        CONSTANT VARCHAR2(30) := 'BRACKET';

/***************************************************************************************************

Setup_Data_Bur: Local procedure called by Setup_Data to set up the test data for the bursting 
                problem 

***************************************************************************************************/
PROCEDURE Setup_Data_Bur (p_point_wide                PLS_INTEGER, -- wide data point
                          p_point_deep                PLS_INTEGER, -- deep data point
                          x_num_records           OUT PLS_INTEGER, -- number of records created
                          x_num_records_per_part  OUT NUMBER,      -- number of records created per partition key
                          x_group_size            OUT NUMBER,      -- group size, where applicable
                          x_text                  OUT VARCHAR2) IS -- descriptive text about the data set

  c_wide_base           CONSTANT PLS_INTEGER := 500;
  c_n_days_in_century   CONSTANT PLS_INTEGER := 36524;
  c_start_date          CONSTANT DATE := To_Date ('01-JAN-1900', 'DD-MON-YYYY');
  c_big_date            CONSTANT VARCHAR2(11) := '01-JAN-3000';
  c_activity            CONSTANT VARCHAR2(10) := 'ANYTHING';
  l_timer               PLS_INTEGER;
  l_cnt_base            NUMBER;
  l_recs_per_person     PLS_INTEGER := c_wide_base * p_point_wide;

  /***************************************************************************************************

  Add_Act: Add a single record to activity table for given person;
           - dates are randomized across a century from 1900

  ***************************************************************************************************/
  PROCEDURE Add_Act (p_person_id PLS_INTEGER) IS -- person id
    l_start_date        DATE := c_start_date + Mod (Abs (DBMS_Random.Random), c_n_days_in_century);
    l_end_date          DATE := l_start_date + Mod (Abs (DBMS_Random.Random), p_point_deep) + 1;
  BEGIN

    INSERT INTO activity (
        activity_id,
        person_id,    
        start_date,    
        end_date,    
        activity_name
    ) VALUES (
        act_s.NEXTVAL,
        p_person_id,
        l_start_date,
        l_end_date,
        c_activity
    );

  END Add_Act;

BEGIN

  l_timer := Timer_Set.Construct ('Setup');
--
-- bench_ctx context can be used to pass bind variables into the queries, eg for the data dimensions
--
  DBMS_SESSION.Set_Context('bench_ctx', 'deep', p_point_deep);

  Utils.g_group_text := 'Setup data : ' || p_point_wide || '-' || p_point_deep;
  EXECUTE IMMEDIATE 'TRUNCATE TABLE activity';
  Utils.Write_Log ('Activity truncated');

  FOR i IN 1..p_point_wide*c_wide_base LOOP

    Add_Act (1);
    Add_Act (2);
    Add_Act (3);

  END LOOP;
  COMMIT;
  Timer_Set.Increment_Time (l_timer, 'Add_Act');

  DBMS_Stats.Gather_Table_Stats (
		ownname			=> 'BENCH',
		tabname			=> 'activity');
  Timer_Set.Increment_Time (l_timer, 'Gather_Table_Stats');
--
-- Query just to provide summary information to log on the data set up
--
  SELECT /* GRP_CNT */
       Round (Count (DISTINCT base_date) / 3, 1)
    INTO l_cnt_base
    FROM (
  SELECT person_id, start_date, end_date, activity_name, activity_id, base_date
    FROM activity
   MODEL
    PARTITION BY (person_id)
    DIMENSION BY (Row_Number() OVER (PARTITION BY person_id ORDER BY start_date) rn)
    MEASURES (start_date, end_date, activity_name, activity_id, start_date base_date)
    RULES (
       base_date[rn = 1] = start_date[cv()],
       base_date[rn > 1] = CASE WHEN start_date[cv()] - base_date[cv()-1] > p_point_deep THEN start_date[cv()] ELSE base_date[cv()-1] END
    )
  );

  Timer_Set.Increment_Time (l_timer, 'GRP_CNT');

  Utils.Write_Log (3 * l_recs_per_person || ' (' || l_recs_per_person || ') records (per person) added, average group size (from) = ' || Round (l_recs_per_person / l_cnt_base, 1) || ' (' ||
    l_recs_per_person || '), # of groups = ' || l_cnt_base);
  Timer_Set.Write_Times (l_timer);

  x_num_records := 3 * l_recs_per_person;
  x_num_records_per_part := l_recs_per_person;
  x_group_size := Round (l_recs_per_person / l_cnt_base, 1);
  x_text := 'Activity test set';

END Setup_Data_Bur;

/***************************************************************************************************

Setup_Data_Bur: Local procedure called by Setup_Data to set up the test data for the item weights
                generalised bursting problem

***************************************************************************************************/
PROCEDURE Setup_Data_Wts (p_point_wide                PLS_INTEGER, -- wide data point
                         p_point_deep                PLS_INTEGER, -- deep data point
                         x_num_records           OUT PLS_INTEGER, -- number of records created
                         x_num_records_per_part  OUT NUMBER,      -- number of records created per partition key
                         x_group_size            OUT NUMBER,      -- group size, where applicable
                         x_text                  OUT VARCHAR2) IS -- descriptive text about the data set

  c_weight_range        CONSTANT PLS_INTEGER := 100;
  c_weight_limit        CONSTANT PLS_INTEGER := 5000;
  c_base_cat            CONSTANT VARCHAR2(30) := 'Cat_';
  l_timer               PLS_INTEGER;
  l_cnt_base            NUMBER;

  /***************************************************************************************************

  Add_Itm: Add a single record to items table for given category;
           - weights are randomized across 1-100

  ***************************************************************************************************/
  PROCEDURE Add_Itm (p_part_no PLS_INTEGER, p_seq PLS_INTEGER) IS -- partition number
  BEGIN

    INSERT INTO items (
        id,
        cat, 
        seq, 
        weight
    ) VALUES (
        itm_s.NEXTVAL,
        c_base_cat || p_part_no, 
        p_seq, 
        Mod (Abs (DBMS_Random.Random), c_weight_range) + 1
    );

  END Add_Itm;

BEGIN

  l_timer := Timer_Set.Construct ('Setup');
--
-- bench_ctx context can be used to pass bind variables into the queries, eg for the data dimensions
--
  DBMS_SESSION.Set_Context('bench_ctx', 'deep', p_point_deep);

  Utils.g_group_text := 'Setup data : ' || p_point_wide || '-' || p_point_deep;
  EXECUTE IMMEDIATE 'TRUNCATE TABLE items';
  Utils.Write_Log ('Items truncated');

  FOR i IN 1..p_point_wide LOOP

    FOR j IN 1..p_point_deep LOOP

      Add_Itm (i, j);

    END LOOP;

  END LOOP;
  COMMIT;
  Timer_Set.Increment_Time (l_timer, 'Add_Itm');

  DBMS_Stats.Gather_Table_Stats (
		ownname			=> 'BENCH',
		tabname			=> 'items');
  Timer_Set.Increment_Time (l_timer, 'Gather_Table_Stats');
--
-- Query just to provide summary information to log on the data set up
-- v11 uses MODEL, v12 MATCH_RECOGNIZE as faster
--
  SELECT /* GRP_CNT */ Count (DISTINCT final_grp) / p_point_wide
    INTO l_cnt_base
    FROM (
  SELECT id,
         cat, 
         seq, 
         weight,
         sub_weight,
         final_grp
    FROM items
   MODEL
     PARTITION BY (cat)
     DIMENSION BY (Row_Number() OVER (PARTITION BY cat ORDER BY seq DESC) rn)
     MEASURES (id, weight, weight sub_weight, id final_grp, seq)
     RULES AUTOMATIC ORDER (
       sub_weight[rn > 1] = CASE WHEN sub_weight[cv()-1] >= c_weight_limit THEN weight[cv()] ELSE sub_weight[cv()-1] + weight[cv()] END,
       final_grp[ANY] = PRESENTV (final_grp[cv()+1], CASE WHEN sub_weight[cv()] >= c_weight_limit THEN id[cv()] ELSE final_grp[cv()+1] END, id[cv()])
     )
  );

  Timer_Set.Increment_Time (l_timer, 'GRP_CNT');

  Utils.Write_Log (p_point_wide * p_point_deep || ' (' || p_point_deep || ') records (per category) added, average group size (from) = ' || Round (p_point_deep / l_cnt_base, 1) || ' (' ||
    p_point_deep || '), # of groups = ' || l_cnt_base);
  Timer_Set.Write_Times (l_timer);

  x_num_records := p_point_wide * p_point_deep;
  x_num_records_per_part := p_point_deep;
  x_group_size := Round (p_point_deep / l_cnt_base, 1);
  x_text := 'Items test set';

END Setup_Data_Wts;

/***************************************************************************************************

Setup_Data_Bur: Local procedure called by Setup_Data to set up the test data for the string
                splitting problem

***************************************************************************************************/
PROCEDURE Setup_Data_Str (p_point_wide                PLS_INTEGER, -- wide data point
                          p_point_deep                PLS_INTEGER, -- deep data point
                          x_num_records           OUT PLS_INTEGER, -- number of records created
                          x_num_records_per_part  OUT NUMBER,      -- number of records created per partition key
                          x_group_size            OUT NUMBER,      -- group size, where applicable
                          x_text                  OUT VARCHAR2) IS -- descriptive text about the data set

  l_base_str                     VARCHAR2(4000);
  l_str_wide                     VARCHAR2(4000);

  c_num_recs            CONSTANT PLS_INTEGER := 3000;
  l_timer                        PLS_INTEGER;

BEGIN

  l_timer := Timer_Set.Construct ('Setup');

  Utils.g_group_text := 'Setup data : ' || p_point_wide || '-' || p_point_deep;
  EXECUTE IMMEDIATE 'TRUNCATE TABLE delimited_lists';
  Utils.Write_Log ('delimited_lists truncated');

  FOR i IN 1..p_point_deep LOOP

    l_base_str := l_base_str || Mod (i, 10);

  END LOOP;

  FOR i IN 1..p_point_wide LOOP
    l_str_wide := l_str_wide || '|' || l_base_str;
  END LOOP;

  FOR i IN 1..c_num_recs LOOP

    INSERT INTO delimited_lists (id, list_col) VALUES (i, Substr (l_str_wide, 2));

  END LOOP;

  COMMIT;
  Timer_Set.Increment_Time (l_timer, 'Insert delimited_lists');

  DBMS_Stats.Gather_Table_Stats (
		ownname			=> 'BENCH',
		tabname			=> 'delimited_lists');
  Timer_Set.Increment_Time (l_timer, 'Gather_Table_Stats');

  Timer_Set.Increment_Time (l_timer, 'GRP_CNT');

  Utils.Write_Log (p_point_wide || ' copies of base string, ' || p_point_deep || ' tokens');
  Timer_Set.Write_Times (l_timer);

  x_num_records := c_num_recs;
  x_num_records_per_part := c_num_recs;
  x_group_size := c_num_recs;
  x_text := 'delimited_lists test set';

END Setup_Data_Str;

/***************************************************************************************************

Setup_Data_Bur: Local procedure called by Setup_Data to set up the test data for the bracket parsing
                problem

***************************************************************************************************/
PROCEDURE Setup_Data_Bra (p_point_wide                PLS_INTEGER, -- wide data point
                          p_point_deep                PLS_INTEGER, -- deep data point
                          x_num_records           OUT PLS_INTEGER, -- number of records created
                          x_num_records_per_part  OUT NUMBER,      -- number of records created per partition key
                          x_group_size            OUT NUMBER,      -- group size, where applicable
                          x_text                  OUT VARCHAR2) IS -- descriptive text about the data set

  l_base_str                     VARCHAR2(4000);
  l_str_wide                     VARCHAR2(4000);

  c_num_recs            CONSTANT PLS_INTEGER := 100;
  l_str                          VARCHAR2(4000);
  l_timer                        PLS_INTEGER;
  FUNCTION Gen_Str (p_point_wide PLS_INTEGER, p_point_deep PLS_INTEGER) RETURN VARCHAR2 IS
    l_str VARCHAR2(4000);
  BEGIN
 
   FOR i IN 1..p_point_deep LOOP
 
     l_str := l_str || '(' || LPad (i, 3, '0');
               
    END LOOP;
 
    FOR i IN 1..p_point_wide-p_point_deep LOOP
 
      l_str := l_str || '(' || LPad (i, 3, '0') || ')';
               
    END LOOP;
 
    FOR i IN 1..p_point_deep LOOP
 
      l_str := l_str || ')';
               
    END LOOP;
    RETURN l_str;
 
  END Gen_Str;
 
BEGIN

  l_timer := Timer_Set.Construct ('Setup');

  Utils.g_group_text := 'Setup data : ' || p_point_wide || '-' || p_point_deep;
  EXECUTE IMMEDIATE 'TRUNCATE TABLE bracket_strings';
  Utils.Write_Log ('bracket_strings truncated');

  l_str := Gen_Str (p_point_wide, p_point_deep);
  FOR i IN 1..c_num_recs LOOP

    INSERT INTO bracket_strings (id, str) VALUES (i, LPad(i, 3, '0') || l_str);

  END LOOP;

  COMMIT;
  Timer_Set.Increment_Time (l_timer, 'Insert bracket_strings');

  DBMS_Stats.Gather_Table_Stats (
		ownname			=> 'BENCH',
		tabname			=> 'bracket_strings');
  Timer_Set.Increment_Time (l_timer, 'Gather_Table_Stats');

  Timer_Set.Increment_Time (l_timer, 'GRP_CNT');

  Utils.Write_Log (p_point_wide || ' copies of base string, ' || p_point_deep || ' tokens');
  Timer_Set.Write_Times (l_timer);

  x_num_records := c_num_recs;
  x_num_records_per_part := c_num_recs;
  x_group_size := c_num_recs;
  x_text := 'bracket_strings test set';

END Setup_Data_Bra;

/***************************************************************************************************

Setup_Data: Set up the test data for a given query group and data point, returning summary info;
            - the spec is fixed, while the body is problem-dependent
            - stats are gathered after the data are created
            - useful summary reports can be included and written to log
            - a timer set can be used to get detailed timings on the setup process
            - four groups are defined in this example, with a local procedure for each group

***************************************************************************************************/
PROCEDURE Setup_Data (  p_query_group               VARCHAR2,    -- query group
                        p_point_wide                PLS_INTEGER, -- wide data point
                        p_point_deep                PLS_INTEGER, -- deep data point
                        x_num_records           OUT PLS_INTEGER, -- number of records created
                        x_num_records_per_part  OUT NUMBER,      -- number of records created per partition key
                        x_group_size            OUT NUMBER,      -- group size, where applicable
                        x_text                  OUT VARCHAR2) IS -- descriptive text about the data set

BEGIN

  IF p_query_group = c_query_group_wts THEN

    Setup_Data_Wts (p_point_wide           => p_point_wide,
                    p_point_deep           => p_point_deep,
                    x_num_records          => x_num_records,
                    x_num_records_per_part => x_num_records_per_part,
                    x_group_size           => x_group_size,
                    x_text                 => x_text);

  ELSIF p_query_group = c_query_group_bur THEN

    Setup_Data_Bur (p_point_wide           => p_point_wide,
                    p_point_deep           => p_point_deep,
                    x_num_records          => x_num_records,
                    x_num_records_per_part => x_num_records_per_part,
                    x_group_size           => x_group_size,
                    x_text                 => x_text);

  ELSIF p_query_group = c_query_group_str THEN

    Setup_Data_Str (p_point_wide           => p_point_wide,
                    p_point_deep           => p_point_deep,
                    x_num_records          => x_num_records,
                    x_num_records_per_part => x_num_records_per_part,
                    x_group_size           => x_group_size,
                    x_text                 => x_text);

  ELSIF p_query_group = c_query_group_bra THEN

    Setup_Data_Bra (p_point_wide           => p_point_wide,
                    p_point_deep           => p_point_deep,
                    x_num_records          => x_num_records,
                    x_num_records_per_part => x_num_records_per_part,
                    x_group_size           => x_group_size,
                    x_text                 => x_text);

  ELSE RAISE_APPLICATION_ERROR (-20001, 'Error, Setup_data procedure not defined for query group ' || p_query_group);

  END IF;

END Setup_Data;

END Bench_Datasets;
/
sho err


