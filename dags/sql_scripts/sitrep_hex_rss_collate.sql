
INSERT INTO disasters_hex
(SELECT dhi.*, ah.gid2 FROM disasters_hex_inter dhi
LEFT JOIN adm2_hex_backup ah
on dhi.h3_08=ah.h3_08)
;

DROP TABLE IF EXISTS disasters_hex_inter;
