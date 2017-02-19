CREATE OR REPLACE PACKAGE BODY Strings AS
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
FUNCTION Split (p_string VARCHAR2, p_delim VARCHAR2) RETURN L1_chr_db_arr PIPELINED IS
/***************************************************************************************************

Split: String splitter

***************************************************************************************************/
  c_delim_len   CONSTANT SIMPLE_INTEGER := Length(p_delim);
  l_token_start          SIMPLE_INTEGER := 1;
  l_next_delim           SIMPLE_INTEGER := Instr (p_string, p_delim, l_token_start, 1);

BEGIN

  WHILE l_next_delim > 0 LOOP
    PIPE ROW (Substr (p_string, l_token_start, l_next_delim - l_token_start));
    l_token_start := l_next_delim + c_delim_len;
    l_next_delim := Instr (p_string || p_delim, p_delim, l_token_start, 1);
  END LOOP;

END Split;

FUNCTION Parse_Brackets (p_str VARCHAR2) RETURN bra_lis_type PIPELINED IS
/***************************************************************************************************

Parse_Brackets: Bracket parser

***************************************************************************************************/
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
    RETURN;
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

        PIPE ROW (bra_rec_type (l_b_lis(l_ob_lis(i)).pos, l_b_lis(l_cb_lis(j)).pos, Substr (p_str, l_b_lis(l_ob_lis(i)).pos, l_b_lis(l_cb_lis(j)).pos - l_b_lis(l_ob_lis(i)).pos + 1)));
        EXIT;

      END IF;

    END LOOP;
 
  END LOOP;

END Parse_Brackets;

END Strings;
/
SHOW ERROR



