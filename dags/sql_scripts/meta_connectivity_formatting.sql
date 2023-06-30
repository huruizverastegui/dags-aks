-- Create disaster table if does not exists

DROP TABLE IF EXISTS meta_connectivity_formatted;


CREATE TABLE meta_connectivity_formatted as (

select event_id, h3_08, date_trunc('week', date) as week_start_date
,

AVG(CASE WHEN data_type='coverage' then value 
ELSE NULL 
END) as "avg_coverage", 

AVG(CASE WHEN data_type='no_coverage' then value 
ELSE NULL 
END) as "avg_no_coverage", 

AVG(CASE WHEN data_type='p_connectivity' then value 
ELSE NULL 
END) as "avg_p_connectivity"

FROM 
(
    
select event_id, name, fromdate, todate, h3_08, data_type, date, value from 

(select d_hex.event_id, d.name, d.fromdate, d.todate, d_hex.h3_08 , mch.data_type, mch.date, mch.value, mch.meta_disaster_id,
row_number() over(partition by d_hex.event_id,d_hex.h3_08,mch.data_type order by date desc) as rn

from disasters_hex d_hex     

left join disasters d 
on d_hex.event_id=d.event_id

left join meta_connectivity_hex mch
on d_hex.h3_08=mch.h3_08

where TRUE 
and mch.date<= d.todate + INTERVAL '28 day'
and mch.date >= d.fromdate

) as sub

where rn=1

) as sub_2

GROUP by 1,2,3)
;

CREATE INDEX ON meta_connectivity_formatted(h3_08)
