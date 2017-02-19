SET LINES 130
SET PAGES 1000
SET TRIMSPOOL ON
SPOOL ..\out\Bracket
EXEC Utils.Clear_Log;
DROP TYPE res_lis_type;
CREATE OR REPLACE TYPE res_rec_type IS OBJECT (o_pos INTEGER, c_pos INTEGER, str VARCHAR2(100));
/
CREATE TYPE res_lis_type IS VARRAY(100) OF res_rec_type;
/
DROP TABLE bracket_strings
/
CREATE TABLE bracket_strings
AS 
      select 1 id, ' ((Hello ( Hi Hi hi ( A B C ( D)) (EF)
why Whwy whyhhh )
)
)' str                                      from dual union all
      select 2, '(1+3*(3-1) + 3*(2+1))' from dual union all
      select 3, '()()*(())a()(())'      from dual union all
      select 7, 'b0(b1(b2(b3(x))(xy)))' from dual
/
COMMIT;

COLUMN id FORMAT 999990
COLUMN token FORMAT A10
COLUMN list_col FORMAT A60

PROMPT Test Data
SELECT * FROM bracket_strings
/
SET TIMING ON
PROMPT WFB_QRY

WITH  FUNCTION prse_str (p_str VARCHAR2) RETURN res_lis_type IS /* WFB_QRY */ 
  c_n_ob       CONSTANT PLS_INTEGER := Length (p_str) - Length (Replace (p_str, '(', ''));
  l_ob_lis              SYS.ODCINumberList := SYS.ODCINumberList();
  l_cb_lis              SYS.ODCINumberList := SYS.ODCINumberList();
  TYPE b_rec_type   IS  RECORD (pos INTEGER, diff INTEGER);
  TYPE b_lis_type   IS  VARRAY(100000) OF b_rec_type;
  l_b_lis               b_lis_type := b_lis_type(NULL);
  l_res_lis             res_lis_type := res_lis_type();
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
  l_res_lis.EXTEND (c_n_ob);
  l_cb_lis.EXTEND (c_n_ob);
  l_b_lis.EXTEND (c_n_ob + c_n_ob);
  FOR i IN 1..c_n_ob LOOP
 
    l_ob_lis(i) := Instr (p_str, '(', 1, i);
    l_cb_lis(i) := Instr (p_str, ')', 1, i);
                DBMS_Output.Put_Line (i || ': ' || LPad (l_ob_lis(i), 2) || ' ' || l_cb_lis(i));
               
  END LOOP;
 
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
    DBMS_Output.Put_Line (l_b_lis(n_b).pos || ': ' || l_b_lis(n_b).diff);
 
  END LOOP;

  FOR i IN 1..n_ob LOOP

    l_o_diff := l_b_lis (l_ob_lis(i)).diff;
    FOR j IN 1..n_cb LOOP

      IF l_b_lis (l_cb_lis(j)).pos < l_b_lis (l_ob_lis(i)).pos THEN CONTINUE; END IF;
      IF l_o_diff = l_b_lis (l_cb_lis(j)).diff + 1 THEN

        l_res_lis(i) := res_rec_type (l_b_lis(l_ob_lis(i)).pos, l_b_lis(l_cb_lis(j)).pos, Substr (p_str, l_b_lis(l_ob_lis(i)).pos, l_b_lis(l_cb_lis(j)).pos - l_b_lis(l_ob_lis(i)).pos + 1));
        EXIT;

      END IF;

    END LOOP;
    DBMS_Output.Put_Line (l_res_lis(i).o_pos || ' - ' || l_res_lis(i).c_pos || ' : ' || l_res_lis(i).str);
 
  END LOOP;
  RETURN l_res_lis;

END;
SELECT  /*+ WFB_QRY gather_plan_statistics */
    b.id, t.o_pos, t.c_pos, t.str
  FROM bracket_strings b
  OUTER APPLY TABLE (prse_str (Nvl (b.str, ' '))) t
 ORDER BY 1, 2
/
EXECUTE Utils.Write_Plan (p_sql_marker => 'WFB_QRY');

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
select   /*+ CBL_QRY gather_plan_statistics */ id, min(l_ct) as l_ct,
        min(case when flag =  1 then pos end) as l_pos,
        min(case when flag = -1 then pos end) as r_pos,
                                Substr (str, min(case when flag =  1 then pos end), min(case when flag = -1 then pos end) - min(case when flag =  1 then pos end) + 1)
from    f
group by id, str, ct, o_ct
order by id, l_ct
;
EXECUTE Utils.Write_Plan (p_sql_marker => 'CBL_QRY');

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
)
select  /*+ MRB_QRY gather_plan_statistics */ 
  id
  ,opening_bracket
  ,closing_bracket
  ,substr(str,opening_bracket,closing_bracket - opening_bracket + 1) t
from
b
MATCH_RECOGNIZE (
partition by id
ORDER BY n
MEASURES 
  str as str
  ,FIRST( N) AS opening_bracket
  ,LAST( N) AS closing_bracket
one ROW PER MATCH
AFTER MATCH SKIP to next row
PATTERN (ob (ob | nb | cb)*? lcb)
DEFINE
  ob as ob.s = '('
  ,cb as cb.s = ')'
  ,nb as nb.s not in ('(',')')
  ,lcb as lcb.s = ')' and (count(ob.s) = count(cb.s) + 1)
) MR
;
EXECUTE Utils.Write_Plan (p_sql_marker => 'MRB_QRY');

SET TIMING OFF
@..\sql\L_Log_Default
SPOOL OFF

