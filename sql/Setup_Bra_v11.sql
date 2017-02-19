DROP TABLE bracket_strings
/
CREATE TABLE bracket_strings (id NUMBER, str VARCHAR2(4000))
/
INSERT INTO  bracket_strings
SELECT 1, ' ((Hello ( Hi Hi hi ( A B C ( D)) (EF)
why Whwy whyhhh )
)
)'                                      FROM DUAL UNION ALL
      SELECT 2, '(1+3*(3-1) + 3*(2+1))' FROM DUAL UNION ALL
      SELECT 3, '()()*(())a()(())'      FROM DUAL UNION ALL
      SELECT 4, 'b0(b1(b2(b3(x))(xy)))' FROM DUAL
/
COMMIT;
/ 

SET TIMING ON
COLUMN id FORMAT 999990
COLUMN str FORMAT A40

PROMPT Test Data
SELECT * FROM bracket_strings
/
SET TIMING ON
PROMPT PFB_QRY

SELECT  /*+ PFB_QRY gather_plan_statistics */
        b.id        id, 
        t.o_pos     o_pos, 
        t.c_pos     c_pos,
        t.str       str
  FROM bracket_strings b
 CROSS JOIN TABLE (Strings.Parse_Brackets (Nvl (b.str, ' '))) t
 ORDER BY b.id, t.o_pos
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'PFB_QRY');

PROMPT CBL_QRY
WITH    d ( id, str, pos ) as (
      select id, str, regexp_instr(str, '\(|\)', 1, level)
      from   bracket_strings
      connect by level <= length(str) - length(translate(str, 'x()', 'x'))
             and prior id = id
             and prior sys_guid() is not null
    ),
    p ( id, str, pos, flag, l_ct, ct ) as (
      select id, str, pos, case substr(str, pos, 1) when '(' then 1 else -1 end,
             sum(case substr(str, pos, 1) when '(' then 1         end) over (partition by id order by pos),
             sum(case substr(str, pos, 1) when '(' then 1 else -1 end) over (partition by id order by pos)
      from   d
    ),
    f ( id, str, pos, flag, l_ct, ct, o_ct ) as (
      select id, str, pos, flag, l_ct, ct + case flag when 1 then 0 else 1 end as ct,
             row_number() over (partition by id, flag, ct order by pos)
      from   p
    )
select   /*+ CBL_QRY gather_plan_statistics */ id,
        min(case when flag =  1 then pos end) as o_pos,
        min(case when flag = -1 then pos end) as c_pos,
                                Substr (str, min(case when flag =  1 then pos end), min(case when flag = -1 then pos end) - min(case when flag =  1 then pos end) + 1) str
from    f
group by id, str, ct, o_ct
order by 1, 2
;
EXECUTE Utils.Write_Plan (p_sql_marker => 'CBL_QRY');
SET TIMING OFF

DECLARE
  c_query_group         CONSTANT VARCHAR2(30) := 'BRACKET';
  c_group_description   CONSTANT VARCHAR2(30) := 'Bracket parsing';
BEGIN

  Bench_Queries.Add_Query (p_query_name => 'WFB_QRY', p_description => 'WITH FUNCTION',
        p_active_yn => 'Y', p_v12_active_only => TRUE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
WITH  FUNCTION Parse_Brackets (p_str VARCHAR2) RETURN bra_lis_type IS /* WFB_QRY */ 
  c_n_ob       CONSTANT PLS_INTEGER := Length (p_str) - Length (Replace (p_str, '(', ''));
  l_ob_lis              SYS.ODCINumberList := SYS.ODCINumberList();
  l_cb_lis              SYS.ODCINumberList := SYS.ODCINumberList();
  TYPE b_rec_type   IS  RECORD (pos INTEGER, diff INTEGER);
  TYPE b_lis_type   IS  VARRAY(32767) OF b_rec_type;
  l_b_lis               b_lis_type := b_lis_type(NULL);
  l_bra_lis             bra_lis_type := bra_lis_type();
  n_b                   PLS_INTEGER := 0;
  n_ob                  PLS_INTEGER := 0;
  n_cb                  PLS_INTEGER := 0;
  l_chr                 VARCHAR2(1);
  l_o_diff              PLS_INTEGER;
BEGIN

  IF c_n_ob = 0 THEN
    RETURN NULL;
  END IF;
  l_ob_lis.EXTEND (c_n_ob);
  l_bra_lis.EXTEND (c_n_ob);
  l_cb_lis.EXTEND (c_n_ob);
  l_b_lis.EXTEND (c_n_ob + c_n_ob);

  FOR i IN 1..Length (p_str) LOOP
 
    l_chr := Substr (p_str, i, 1);
    IF l_chr NOT IN ('(', ')') THEN CONTINUE; END IF;

    n_b := n_b + 1;
    l_b_lis(n_b).pos := i;
 
    IF l_chr = '(' THEN
      n_ob := n_ob + 1;
      l_ob_lis(n_ob) := n_b;
    ELSE
      n_cb := n_cb + 1;
      l_cb_lis(n_cb) := n_b;
    END IF;

    l_b_lis(n_b).diff := n_ob - n_cb;
 
  END LOOP;

  FOR i IN 1..n_ob LOOP

    l_o_diff := l_b_lis (l_ob_lis(i)).diff;
    FOR j IN 1..n_cb LOOP

      IF l_b_lis (l_cb_lis(j)).pos < l_b_lis (l_ob_lis(i)).pos THEN CONTINUE; END IF;
      IF l_o_diff = l_b_lis (l_cb_lis(j)).diff + 1 THEN

        l_bra_lis(i) := bra_rec_type (l_b_lis(l_ob_lis(i)).pos, l_b_lis(l_cb_lis(j)).pos, Substr (p_str, l_b_lis(l_ob_lis(i)).pos, l_b_lis(l_cb_lis(j)).pos - l_b_lis(l_ob_lis(i)).pos + 1));
        EXIT;

      END IF;

    END LOOP;
 
  END LOOP;
  RETURN l_bra_lis;

END;
SELECT
/* SEL */
        b.id        id, 
        t.o_pos     o_pos, 
        t.c_pos     c_pos,
        t.str       str
/* SEL */
  FROM bracket_strings b
  OUTER APPLY TABLE (Parse_Brackets (Nvl (b.str, ' '))) t
 ORDER BY b.id, t.o_pos
!');

  Bench_Queries.Add_Query (p_query_name => 'CBL_QRY', p_description => 'Connect By', p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
WITH    d ( id, str, pos ) as (
      select id, str, regexp_instr(str, '\(|\)', 1, level)
      from   bracket_strings
      connect by level <= length(str) - length(translate(str, 'x()', 'x'))
             and prior id = id
             and prior sys_guid() is not null
    ),
    p ( id, str, pos, flag, l_ct, ct ) as (
      select id, str, pos, case substr(str, pos, 1) when '(' then 1 else -1 end,
             sum(case substr(str, pos, 1) when '(' then 1         end) over (partition by id order by pos),
             sum(case substr(str, pos, 1) when '(' then 1 else -1 end) over (partition by id order by pos)
      from   d
    ),
    f ( id, str, pos, flag, l_ct, ct, o_ct ) as (
      select id, str, pos, flag, l_ct, ct + case flag when 1 then 0 else 1 end as ct,
             row_number() over (partition by id, flag, ct order by pos)
      from   p
    )
SELECT
/* SEL */
        id        id, 
        min(case when flag =  1 then pos end) o_pos,
        min(case when flag = -1 then pos end) c_pos,
        Substr (str, min(case when flag =  1 then pos end), min(case when flag = -1 then pos end) - min(case when flag =  1 then pos end) + 1)       str
/* SEL */
from    f
group by id, str, ct, o_ct
order by id, min(case when flag =  1 then pos end)
!');

  Bench_Queries.Add_Query (p_query_name => 'MRB_QRY', p_description => 'Match Recognize', p_active_yn => 'Y', p_v12_active_only => TRUE, p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
WITH b as
(
select
  substr(str,level,1) s
  ,level n
  ,id
  ,str
from
  bracket_strings
connect by
id =  prior id
and substr(str,level,1) is not null
and prior sys_guid() is not null
), mrq as (
select
  id
  ,o_pos
  ,c_pos
  ,substr(str,o_pos,c_pos - o_pos + 1) t
from
b
MATCH_RECOGNIZE (
partition by id
ORDER BY n
MEASURES 
  str as str
  ,FIRST( N) AS o_pos
  ,LAST( N) AS c_pos
one ROW PER MATCH
AFTER MATCH SKIP to next row
PATTERN (ob (ob | nb | cb)*? lcb)
DEFINE
  ob as ob.s = '('
  ,cb as cb.s = ')'
  ,nb as nb.s not in ('(',')')
  ,lcb as lcb.s = ')' and (count(ob.s) = count(cb.s) + 1)
) MR
)
SELECT
/* SEL */
        id        id, 
        o_pos o_pos,
        c_pos c_pos,
        t       str
/* SEL */
from    mrq
order by id, o_pos
!');

  Bench_Queries.Add_Query (p_query_name => 'PFB_QRY', p_description => 'Pipelined function', p_active_yn => 'Y', p_query_group => c_query_group, p_group_description => c_group_description, p_text =>
q'!
SELECT 
/* SEL */
        b.id        id, 
        t.o_pos     o_pos, 
        t.c_pos     c_pos,
        t.str       str
/* SEL */
  FROM bracket_strings b
  CROSS JOIN TABLE (Strings.Parse_Brackets (b.str)) t
 ORDER BY b.id, t.o_pos
!');

END;
/

