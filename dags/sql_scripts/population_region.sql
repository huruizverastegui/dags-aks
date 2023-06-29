-- Create disaster table if does not exists

DROP TABLE IF EXISTS population_by_region;

CREATE TABLE population_by_region as (
SELECT disasters_hex.event_id ,
adm2_hex.gid2 AS gid2,
eapro_adm2."NAME_2" AS "NAME_2", 
sum(population_crosstab.children_under_five) AS children_under_five, 
sum(population_crosstab.elderly_60_plus) AS elderly_60_plus, 
sum(population_crosstab.men) AS men, 
sum(population_crosstab.women) AS women, 
sum(population_crosstab.women_of_reproductive_age_15_49) AS women_of_reproductive_age_15_49, 
sum(population_crosstab.youth_15_24) AS youth_15_24, 
sum(population_crosstab.general) AS general, 
sum(schools.count) AS school_count 

FROM population_crosstab 
JOIN disasters_hex ON population_crosstab.h3_08 = disasters_hex.h3_08 
JOIN adm2_hex ON disasters_hex.h3_08 = adm2_hex.h3_08 
JOIN eapro_adm2 ON adm2_hex.gid2 = eapro_adm2."GID_2" 
JOIN schools ON schools.h3_08 = adm2_hex.h3_08 
GROUP BY disasters_hex.event_id, adm2_hex.gid2, eapro_adm2."NAME_2" 
ORDER BY children_under_five DESC 
);

CREATE INDEX ON population_by_region(event_id)
