-- Create disaster table if does not exists
-- DROP TABLE IF EXISTS disasters;

CREATE TABLE IF NOT EXISTS disasters (
	"event_id" int NULL,
	"fromdate" date NULL,
	"todate" date NULL,
	"iscurrent" text NULL,
	"eventtype" text NULL,
	"alertscore" float NULL,
	"name" text NULL,
	"country" text NULL,
	"rss_url" text NULL,
	"report_url" text NULL,
	"bbox" geometry NULL,
	"geo_url" text NULL,
	"geometry" geometry NULL,
	"geometry_validated" geometry NULL,
	"update_date" timestamp NULL,
	"htmldescription" text NULL
	--,"h3_list" text NULL
	--"geometry" geometry NULL
);

CREATE INDEX ON disasters(event_id)

-- delete content of table to avoid duplicates
 --TRUNCATE TABLE disasters;
