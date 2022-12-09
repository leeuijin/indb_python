#!/bin/bash

###### query start
psql -d skon -e <<-!

select a.lot_id, a.cell_id, a.param_id
      , a.timestamp_id
      , round(a.measure_val, 4) raw
      , round(b.measure_val::numeric, 4) local_py
      , round(c.measure_val, 4) indb_py
from   equipment.ts_data a
     , equipment.ts_data_local_ml b
     , (select lot_id, cell_id, param_id
             , unnest(timestamp_id_arr) timestamp_id
             , unnest(measure_val_arr) measure_val
        from  equipment.ts_data_indb_ml
        where lot_id = 1 
       ) c
where a.lot_id = b.lot_id
and   a.cell_id = b.cell_id
and   a.param_id  = b.param_id
and   a.timestamp_id = b.timestamp_id
and   a.lot_id  =c.lot_id
and   a.cell_id = c.cell_id
and   a.param_id  = c.param_id
and   a.timestamp_id = c.timestamp_id
and   a.lot_id = 1
and   a.cell_id = 1
order by 1, 2, 3, 4
;

!
###### query end

