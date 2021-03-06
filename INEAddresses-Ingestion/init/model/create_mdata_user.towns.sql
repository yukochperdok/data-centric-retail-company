DROP TABLE IF EXISTS mdata_user.towns;

CREATE TABLE mdata_user.towns(
	town_code STRING NOT NULL COMMENT 'Town code',
	land1 STRING NOT NULL COMMENT 'Country code',
	spras STRING NOT NULL COMMENT 'Language',
	bland STRING NOT NULL COMMENT 'Province code',
	codauto STRING NOT NULL COMMENT 'CCAA code',
	town_name STRING NOT NULL COMMENT 'Town name',
	ine_modification_date TIMESTAMP NOT NULL COMMENT 'INE modification date',
	ts_insert_dlk TIMESTAMP NOT NULL COMMENT 'Insert date',
	user_insert_dlk STRING NOT NULL COMMENT 'Insert user',
	ts_update_dlk TIMESTAMP COMMENT 'Last modification date',
	user_update_dlk STRING COMMENT 'Last modification user',
	PRIMARY KEY(town_code, land1, spras)
)
COMMENT 'Towns master table'
STORED AS KUDU;
