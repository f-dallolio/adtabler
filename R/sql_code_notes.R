'
CREATE TABLE "{tbl_name}" (

  "{col_name_std}" {sql_datatype},
  "{col_name_std}" {sql_datatype},

  PRIMARY KEY ("{pk}", "{pk}")
) PARTITION BY RANGE (ad_date);
'

"
CREATE TABLE { tbl_name }_y{ year } PARTITION OF {tbl_name}
    FOR VALUES FROM ('{ date_from }') TO ('{ date_to }')
    PARTITION BY RANGE ({ media_type_id });

CREATE TABLE measurement_y2007m12 PARTITION OF {tbl_name}
    FOR VALUES FROM ('2007-12-01') TO ('2008-01-01')
    TABLESPACE fasttablespace;

CREATE TABLE measurement_y2008m01 PARTITION OF {tbl_name}
    FOR VALUES FROM ('2008-01-01') TO ('2008-02-01')
    WITH (parallel_workers = 4)
    TABLESPACE fasttablespace;
"

"
CREATE TABLE measurement_y2006m02 PARTITION OF measurement
    FOR VALUES FROM ('2006-02-01') TO ('2006-03-01')
    PARTITION BY RANGE (peaktemp);
"
