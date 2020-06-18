DROP TABLE IF EXISTS mdata_user.zip_codes;

CREATE TABLE mdata_user.zip_codes(
	zip_code STRING NOT NULL COMMENT 'Zip code',
	town_code STRING NOT NULL COMMENT 'Town code',
	land1 STRING NOT NULL COMMENT 'Country code',
	bland STRING NOT NULL COMMENT 'Province code',
	codauto STRING NOT NULL COMMENT 'CCAA code',
	ine_modification_date TIMESTAMP NOT NULL COMMENT 'INE modification date',
	ts_insert_dlk TIMESTAMP NOT NULL COMMENT 'Insert date',
	user_insert_dlk STRING NOT NULL COMMENT 'Insert user',
	ts_update_dlk TIMESTAMP COMMENT 'Last modification date',
	user_update_dlk STRING COMMENT 'Last modification user',
    PRIMARY KEY(zip_code, town_code, land1)
)
COMMENT 'Zip codes master table'
STORED AS KUDU;
