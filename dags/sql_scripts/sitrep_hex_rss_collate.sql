
INSERT INTO disasters_hex
(SELECT dhi.*, ah.gid1, ah.gid2 FROM disasters_hex_inter dhi

INNER JOIN adm2_hex ah
on dhi.h3_08=ah.h3_08)
;

DROP TABLE IF EXISTS disasters_hex_inter;
