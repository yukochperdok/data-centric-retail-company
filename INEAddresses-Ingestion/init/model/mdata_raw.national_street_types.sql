DROP TABLE IF EXISTS mdata_raw.national_street_types;
CREATE EXTERNAL TABLE mdata_raw.national_street_types
(
    previous_bland                     STRING          COMMENT 'Previous province code',
    previous_local_council_code        STRING          COMMENT 'Previous municipality code',
    previous_street_code               INT             COMMENT 'Previous street code',
    information_type                   CHAR(1)         COMMENT 'Information type',
    refund_cause                       STRING          COMMENT 'Refund cause',
    INE_modification_date              TIMESTAMP       COMMENT 'INE modification date',
    INE_modification_code              CHAR(1)         COMMENT 'INE modification code',
    street_code                        INT             COMMENT 'Street code',
    street_type                        STRING          COMMENT 'Street type',
    street_type_position               INT             COMMENT 'Indicates if street type is a prefix (0) or a suffix (1)',
    street_name                        STRING          COMMENT 'Street name',
    short_street_name                  STRING          COMMENT 'Short street name',
    ts_insert_dlk                      TIMESTAMP       COMMENT 'Inserted date',
    user_insert_dlk                    STRING          COMMENT 'User that inserted the registry',
    ts_update_dlk                      TIMESTAMP       COMMENT 'Updated date',
    user_update_dlk                    STRING          COMMENT 'User that updated the registry'
)
PARTITIONED BY
(
    year                               INT             COMMENT 'Year of INE file',
    month                              INT             COMMENT 'Month of INE file'
)
COMMENT 'INE street types historification'
STORED AS PARQUET;
