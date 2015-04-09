CREATE OR REPLACE FUNCTION array_agg_custom_cut(anyarray, anyarray)
RETURNS anyarray AS $$
   SELECT array_cat($1, ARRAY[$2]);
$$ LANGUAGE SQL;

DROP AGGREGATE IF EXISTS array_agg_cust(anyarray);
CREATE AGGREGATE array_agg_cust(anyarray)
(
  SFUNC = array_agg_custom_cut,
  STYPE = anyarray
);

