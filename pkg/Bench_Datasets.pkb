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
Brendan Furey        05-Nov-2016 1.0   Created

***************************************************************************************************/

/***************************************************************************************************

Setup_Data: Set up the test data for a given query group and data point, returning summary info;
            - the spec is fixed, while the body is problem-dependent
            - stats are gathered after the data are created
            - useful summary reports can be included and written to log
            - a timer set can be used to get detailed timings on the setup process
            - demo version for the bursting problem

***************************************************************************************************/
PROCEDURE Setup_Data (  p_query_group               VARCHAR2,    -- query group
                        p_point_wide                PLS_INTEGER, -- wide data point
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
  l_timer_stat_rec      Timer_Set.timer_stat_rec;
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

END Setup_Data;

END Bench_Datasets;
/
sho err


