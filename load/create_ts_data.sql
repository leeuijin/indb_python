drop table equipment.ts_data;

create table equipment.ts_data (
lot_id   int,
cell_id  int,
param_id int,
timestamp_id int,
measure_val numeric
)
distributed by (lot_id) ;

CREATE TABLE equipment.ts_data_local_ml (
        lot_id int4 NULL,
        cell_id int4 NULL,
        param_id int4 NULL,
        timestamp_id int4 NULL,
        measure_val float4 NULL
)
DISTRIBUTED BY (lot_id);

CREATE TABLE equipment.ts_data_indb_ml (
	lot_id int4 NULL,
	cell_id int4 NULL,
	param_id int4 NULL,
	timestamp_id_arr _int4 NULL,
	measure_val_arr _numeric NULL
)
DISTRIBUTED BY (lot_id);
