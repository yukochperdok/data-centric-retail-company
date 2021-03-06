DROP TABLE IF EXISTS mdata_raw.national_street_stretches;
CREATE EXTERNAL TABLE mdata_raw.national_street_stretches
(
    previous_bland                     STRING          COMMENT 'Previous province code',
    previous_local_council_code        STRING          COMMENT 'Previous municipality code',
    previous_district                  STRING          COMMENT 'Previous district',
    previous_section                   STRING          COMMENT 'Previous section',
    previous_section_letter            CHAR(1)         COMMENT 'Previous section letter',
    previous_subsection                STRING          COMMENT 'Previous subsection',
    previous_cun                       STRING          COMMENT 'Previous population code',
    previous_street_code               INT             COMMENT 'Previous street code',
    previous_pseudostreet_code         INT             COMMENT 'Previous pseudostreet code',
    previous_square                    STRING          COMMENT 'Previous square',
    previous_zip_code                  STRING          COMMENT 'Previous zip code',
    previous_numbering_type            STRING          COMMENT 'Indicates if previous street stretch was even/odd or did not have any number',
    previous_initial_number            INT             COMMENT 'Previous stretch initial number',
    previous_initial_letter            CHAR(1)         COMMENT 'Previous stretch initial letter',
    previous_end_number                INT             COMMENT 'Previous stretch end number',
    previous_end_letter                CHAR(1)         COMMENT 'Previous stretch end letter',
    information_type                   CHAR(1)         COMMENT 'Information type',
    refund_cause                       STRING          COMMENT 'Refund cause',
    INE_modification_date              TIMESTAMP       COMMENT 'INE modification date',
    INE_modification_code              CHAR(1)         COMMENT 'INE modification code',
    district                           STRING          COMMENT 'District',
    section                            STRING          COMMENT 'Section',
    section_letter                     CHAR(1)         COMMENT 'Section letter',
    subsection                         STRING          COMMENT 'Subsection',
    cun                                STRING          COMMENT 'Population code',
    group_entity_short_name            STRING          COMMENT 'Group entity short name',
    town_name                          STRING          COMMENT 'Town name',
    last_granularity_short_name        STRING          COMMENT 'Last granularity short name of town',
    street_code                        INT             COMMENT 'Street code',
    street_name                        STRING          COMMENT 'Street name',
    pseudostreet_code                  INT             COMMENT 'Pseudostreet code',
    pseudostreet_name                  STRING          COMMENT 'Pseudostreet name',
    cadastral_square                   STRING          COMMENT 'Cadastral Square',
    zip_code                           STRING          COMMENT 'Zip code',
    numbering_type                     STRING          COMMENT 'Indicates if street stretch is even, odd or does not have any number',
    initial_number                     INT             COMMENT 'Stretch initial number (can contain letters)',
    initial_letter                     CHAR(1)         COMMENT 'Stretch initial letter',
    end_number                         INT             COMMENT 'Stretch end number',
    end_letter                         CHAR(1)         COMMENT 'Stretch end letter',
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
COMMENT 'INE street stretches historification'
STORED AS PARQUET;
