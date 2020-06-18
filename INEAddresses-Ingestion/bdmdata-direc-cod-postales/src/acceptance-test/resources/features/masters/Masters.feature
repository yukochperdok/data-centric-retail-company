Feature: Postal Codes and towns masters
  @acc
  Scenario Outline: Should throw an alarm when any input table is not available
    Given a <scenario> with only previous data of ccaa
    When any input entity is not available in <path>
    Then an <alarm> should be thrown and nothing should be written <towns_table> and <postal_codes_table>

    Examples:
    |scenario|path|alarm|towns_table|postal_codes_table|
    |scenario1|input|A - The impala::mdata_user.zip_codes_street_map table is not available|towns_table.csv|postal_codes_table.csv|

  @acc
  Scenario Outline: Should throw an alarm when any input table is not available
    Given a <scenario> with only previous data of street map
    When any input entity is not available in <path>
    Then an <alarm> should be thrown and nothing should be written <towns_table> and <postal_codes_table>

    Examples:
    |scenario|path|alarm|towns_table|postal_codes_table|
    |scenario2|input|A - The impala::mdata_user.ccaa_regions table is not available|towns_table.csv|postal_codes_table.csv|

  @acc
  Scenario Outline: Should insert all postal codes and towns from street map
    Given a <scenario> with previous data of street map and ccaa
    When any input entity is available in <path>
    Then all postal codes and towns should be inserted plus audit fields in <towns_table> and <postal_codes_table>

    Examples:
    |scenario|path|towns_table|postal_codes_table|
    |scenario3|input|towns_table.csv|postal_codes_table.csv| # with data in output tables
    |scenario4|input|towns_table.csv|postal_codes_table.csv| # without data in output tables

  @acc
  Scenario Outline: Should trace logs when an error occurs
    Given a <scenario> with previous data of street map and ccaa
    When any output entity is not available in <path>
    Then an <alarm> should be thrown and should be written <towns_table> and <postal_codes_table>

    Examples:
    |scenario|path|alarm|towns_table|postal_codes_table|
    |scenario5|input|A - The towns entity is not available|towns_table.csv|postal_codes_table.csv|
    |scenario6|input|A - The Postal codes entity is not available|towns_table.csv|postal_codes_table.csv|

  @acc
  Scenario Outline: Should insert all postal codes and towns from street map and trace logs
    Given a <scenario> with previous data of street map and ccaa
    When any input entity is available in <path> and there are province codes in street map but not in ccaa
    Then process should be traced with <message> and those provinces should not be written in <towns_table> and <postal_codes_table>

    Examples:
    |scenario|path|message|towns_table|postal_codes_table|
    |scenario7|input|I - The province is not available|towns_table.csv|postal_codes_table.csv|