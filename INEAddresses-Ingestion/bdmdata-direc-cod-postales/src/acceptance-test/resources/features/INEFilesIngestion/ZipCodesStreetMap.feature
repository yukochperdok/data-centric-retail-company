Feature: Street Stretches Ingestion and Zip Codes Street Map generation
  @acc
  Scenario Outline: Should insert all street stretches from street stretches INE file
   Given a <scenario> with previous data of street stretches
   When a correct street stretches file is found in <path>
   Then all street stretches should be historified with all file information plus audit fields in <historified_raw_table>, and in <historified_last_table>, Zip Codes Street Map should be generated in <zip_codes_street_map_table>

   Examples:
   |scenario|path|historified_raw_table|historified_last_table|zip_codes_street_map_table|
   |scenario1|input|historified_raw_table.csv|historified_last_table.csv|zip_codes_street_map.csv| # with all tables empty
   |scenario2|input|historified_raw_table.csv|historified_last_table.csv|zip_codes_street_map.csv| # with data for the same partition
   |scenario3|input|historified_raw_table.csv|historified_last_table.csv|zip_codes_street_map.csv| # with data for other partitions
   |scenario4|input|historified_raw_table.csv|historified_last_table.csv|zip_codes_street_map.csv| # with data for both same partition and others

  @acc
  Scenario Outline: Should insert all street stretches from street stretches INE file with special characters and generate Zip Codes Street Map
   Given a <scenario> with previous data of street stretches
   When a correct street stretches file with special characters in street names is found in <path>
   Then all street stretches should be historified with all file information including those special characters plus audit fields in <historified_raw_table>, and in <historified_last_table>, Zip Codes Street Map should be generated in <zip_codes_street_map_table>

   Examples:
   |scenario|path|historified_raw_table|historified_last_table|zip_codes_street_map_table|
   |scenario5|input|historified_raw_table.csv|historified_last_table.csv|zip_codes_street_map.csv|

  @acc
  Scenario Outline: Should insert all street stretches from street stretches INE file, generate Zip Codes Street Map and trace logs
   Given a <scenario> with previous data of street stretches
   When a correct street stretches file is found in <path>
   Then all street stretches should be historified, the zip codes street map should be generated and process should be traced with <ending>

   Examples:
   |scenario|path|ending|
   |scenario1|input|FP - INEFilesIngestion finished correctly|

  @acc
  Scenario Outline: Should trace logs when an error occurs, for instance wrong file name
   Given a <scenario> with previous data of street stretches
   When a street stretches file that does not match the file pattern in <path>
   Then an <alarm> should be thrown and nothing should be written in <historified_raw_table> or <historified_last_table> or <zip_codes_street_map_table>

   Examples:
   |scenario|path|alarm|historified_raw_table|historified_last_table|zip_codes_street_map_table|
   |scenario6|input|A - No file with correct name has been found, so no street stretches have been inserted|historified_raw_table.csv|historified_last_table.csv|zip_codes_street_map.csv|

  @acc
  Scenario Outline: Should trace logs when an error occurs, for instance wrong number of characters in lines
   Given a <scenario> with previous data of street stretches
   When a street stretches file that contains wrong number of lines in <path>
   Then an <alarm> should be thrown and nothing should be written in <historified_raw_table> or <historified_last_table> or <zip_codes_street_map_table>

   Examples:
   |scenario|path|alarm|historified_raw_table|historified_last_table|zip_codes_street_map_table|
   |scenario7|input|A - File could not be parsed correctly, so no street stretches have been inserted|historified_raw_table.csv|historified_last_table.csv|zip_codes_street_map.csv|

  @acc
  Scenario Outline: Should insert all street stretches from street stretches INE file and filtering widespread registries before generating the Zip Codes Street Map
   Given a <scenario> with previous data of street stretches
   When a correct street stretches file is found in <path>
   Then all street stretches should be historified with all file information plus audit fields in <historified_raw_table>, and in <historified_last_table>, Zip Codes Street Map should be generated in <zip_codes_street_map_table>

   Examples:
   |scenario|path|historified_raw_table|historified_last_table|zip_codes_street_map_table|
   |scenario8|input|historified_raw_table.csv|historified_last_table.csv|zip_codes_street_map.csv|

  @acc
  Scenario Outline: Should insert all street stretches belonging to same street but with different initial number and end number into street stretches tables and street map table
   Given a <scenario> with previous data of street stretches
   When a correct street stretches file is found in <path>
   Then all street stretches should be historified with all file information plus audit fields in <historified_raw_table>, and in <historified_last_table>, Zip Codes Street Map should be generated in <zip_codes_street_map_table>

   Examples:
   |scenario|path|historified_raw_table|historified_last_table|zip_codes_street_map_table|
   |scenario9|input|historified_raw_table.csv|historified_last_table.csv|zip_codes_street_map.csv|

   @acc
   Scenario Outline: Should insert all street stretches only into street stretches historic table when manual flag is indicated
    Given a <scenario> with previous data of street stretches
    When a correct street stretches file is found in <path> with historify flag active
    Then all street stretches should be historified with all file information plus audit fields in <historified_raw_table>, but not in <historified_last_table>, nor in <zip_codes_street_map_table>

    Examples:
    |scenario|path|historified_raw_table|historified_last_table|zip_codes_street_map_table|
    |scenario10|input|historified_raw_table.csv|historified_last_table.csv|zip_codes_street_map.csv|

   @acc
   Scenario Outline: Should not insert any street stretches into street stretches tables nor into street map when information to be inserted is obsolete
    Given a <scenario> with previous data of street stretches
    When an older street stretches file is found in <path>
    Then an <alarm> should be thrown and nothing should be written in <historified_raw_table> or <historified_last_table> or <zip_codes_street_map_table>

    Examples:
    |scenario|path|historified_raw_table|historified_last_table|zip_codes_street_map_table|
    |scenario11|input|historified_raw_table.csv|historified_last_table.csv|zip_codes_street_map.csv|