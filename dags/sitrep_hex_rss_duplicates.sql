
DROP TABLE IF EXISTS disasters_hex_deduplicated;


CREATE TABLE disasters_hex_deduplicated
( event_id int, eventtype text, h3_08 text, update_date timestamp, last_update timestamp, gid2 varchar);
	
with latest_updates as(
	select event_id, max(update_date) as last_update
	from disasters_hex
	group by 1), 
	
	hex_joined as (
	select dh.event_id, dh.eventtype, dh.h3_08, dh.update_date, lu.last_update, dh.gid2  from disasters_hex dh 
	left join latest_updates lu
	on dh.event_id = lu.event_id
	)
	
insert into disasters_hex_deduplicated
select * from hex_joined;
	
DELETE FROM disasters_hex_deduplicated where update_date<last_update;
DELETE FROM disasters_hex_deduplicated where h3_08 is Null;
ALTER TABLE disasters_hex_deduplicated DROP COLUMN last_update;

DROP TABLE disasters_hex;

ALTER TABLE disasters_hex_deduplicated RENAME TO disasters_hex;

--INSERT INTO disasters_hex SELECT event_id ,
--eventtype ,
--h3_08,
--update_date,
--gid2

--FROM disasters_hex_deduplicated;

--DROP TABLE IF EXISTS disasters_hex_deduplicated;