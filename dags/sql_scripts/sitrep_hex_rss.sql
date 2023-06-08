
CREATE TABLE IF NOT EXISTS disasters_hex (
	"event_id" int NULL,
	"eventtype" text NULL,
	"h3_08" text NULL,
	"update_date" timestamp NULL,
	"gid2" varchar NULL
); 

DROP TABLE IF EXISTS disasters_hex_inter;

CREATE TABLE  disasters_hex_inter (
	"event_id" int NULL,
	"eventtype" text NULL,
	"h3_08" text NULL,
	"update_date" timestamp NULL
); 
--CREATE INDEX ON disasters_hex(event_id, h3)

