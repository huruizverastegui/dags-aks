DROP TABLE IF EXISTS disasters_deduplicated;

CREATE TABLE disasters_deduplicated
AS (select *, ROW_NUMBER() OVER (
            PARTITION BY 
                event_id, 
        		eventtype
        
            ORDER BY 
                update_date desc
        ) as duplicate_check

	from disasters);

CREATE INDEX ON disasters(event_id, update_date) ;

DELETE FROM disasters_deduplicated where duplicate_check > 1;

TRUNCATE TABLE disasters;
INSERT INTO disasters SELECT event_id ,
fromdate ,
todate ,
iscurrent ,
eventtype ,
alertscore,
name ,
country ,
rss_url ,
report_url ,
bbox,
geo_url ,
geometry ,
geometry_validated ,
update_date,
htmldescription
--,"h3_list" 
FROM disasters_deduplicated;

DROP TABLE IF EXISTS disasters_deduplicated;


