DROP TABLE IF EXISTS mdata_user.zip_codes_street_map;
CREATE TABLE mdata_user.zip_codes_street_map (
    spras                  STRING    NOT NULL COMMENT 'Language',
    land1                  STRING    NOT NULL COMMENT 'Country',
    bland                  STRING    NOT NULL COMMENT 'Province code',
    local_council_code     STRING    NOT NULL COMMENT 'Municipality code',
    cun                    STRING    NOT NULL COMMENT 'Population code',
    street_code            INT       NOT NULL COMMENT 'Street code',
    zip_code               STRING    NOT NULL COMMENT 'Zip code',
    numbering              STRING    NOT NULL COMMENT 'Type of numbering',
    initial_number         INT       NOT NULL COMMENT 'Stretch initial number',
    end_number             INT       NOT NULL COMMENT 'Stretch end number',
    initial_letter         STRING             COMMENT 'Letter of the initial number of the stretch',
    end_letter             STRING             COMMENT 'Letter of the end number of the stretch',
    town_code              STRING             COMMENT 'Town code',
    street_name            STRING             COMMENT 'Street name',
    town_name              STRING             COMMENT 'Town name',
    ine_modification_date  TIMESTAMP          COMMENT 'INE modification date',
    ts_insert_dlk          TIMESTAMP NOT NULL COMMENT 'Inserted date',
    user_insert_dlk        STRING    NOT NULL COMMENT 'User that inserted the registry',
    ts_update_dlk          TIMESTAMP NOT NULL COMMENT 'Updated date',
    user_update_dlk        STRING    NOT NULL COMMENT 'User that updated the registry',
    street_type            STRING             COMMENT 'Street type',
    is_prefix_else_suffix  BOOLEAN            COMMENT 'Indicates if street type is a prefix (true) or a suffix (false)',
    PRIMARY KEY (spras, land1, bland, local_council_code, cun, street_code, zip_code, numbering, initial_number, end_number, initial_letter, end_letter)
)
PARTITION BY HASH(street_code, zip_code) PARTITIONS 3
COMMENT 'Callejero'
STORED AS KUDU;