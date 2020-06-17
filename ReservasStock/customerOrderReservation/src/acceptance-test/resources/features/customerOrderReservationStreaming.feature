Feature: E-commerce orders to customer orders reservations

  @acc
  Scenario Outline: Should treat new orders and orders modifications and update customer orders reservations table
    Given events from <scenario>
    When the streaming is executed given that <scenario>
    Then it will insert reservations info in customer orders reservations table <scenario>

    Examples:
      |scenario|
      ### Cases with NON VARIABLE WEIGHT articles ###
      # Events with new and modification order when they DO NOT exist on customers orders tables: Both Food and Non Food cases
      |scenario1A|
      # Events with new and modification order when they DO exist on customers orders tables: Both Food and Non Food cases
      |scenario1B|
      ### Cases with just VARIABLE WEIGHT articles ###
      # Events with new and modified orders: Both Food and Non Food cases
      |scenario1C|
      ### Cases with several articles per order ###
      # Events with new and modified orders: Both Food and Non Food cases
      |scenario1D|
      ### Cases with accents and lowercase ###
      # Events with new and modified orders: Both Food and Non Food cases
      |scenario1E|

  @acc
  Scenario Outline: Should delete reservations whose orders are in finished states based on reservations actions table
    Given events from <scenario>
    When the streaming is executed given that <scenario>
    Then it will delete reservations info in customer orders reservations table <scenario>

    Examples:
      |scenario|
      # When orders to be deleted are customer orders reservations table: Both Food and Non Food cases
      |scenario2A|
      # When orders to be deleted are not in customer orders reservations table (i.e. are discarded): Both Food and Non Food cases
      |scenario2B|
      # When there are both orders to be deleted and to be inserted: Both Food and Non Food cases
      |scenario2C|


  @acc
  Scenario Outline: Should discard orders from marketplace
    Given events from <scenario>
    When the streaming is executed given that <scenario>
    Then it will not insert reservations info in customer orders reservations table <scenario>

    Examples:
      |scenario|
      # Includes events from marketplace and one event not from marketplace
      |scenario3|

  @acc
  Scenario Outline: Should discard new orders and orders modifications when article or location could not be transcoded
    Given events from <scenario>
    When the streaming is executed given that <scenario>
    Then it will insert reservations info in customer orders reservations table <scenario> and trace error <error>

    Examples:
      |scenario|error|
      # One valid event and another with article not in transcodification table
      |scenario4A|A - Article not found when transcoding|
      # One valid event and another with store not in transcodification site table
      |scenario4B|A - Store not found when transcoding:|

  @acc
  Scenario Outline: Should discard new orders and orders modifications when no article or location is set
    Given events from <scenario>
    When the streaming is executed given that <scenario>
    Then it will insert reservations info in customer orders reservations table <scenario> and trace error <error>

    Examples:
      |scenario|error|
      # One valid event and another with null store
      |scenario5A|Customer order reservation with store null:|
      # One valid event and another with null article
      |scenario5B|Customer order reservation with article null:|
      # One valid event and another with both null article and store. In this case, first validation is printed and the rest is ignored
      |scenario5C|Customer order reservation with store null:|

  @acc
  Scenario Outline: Should discard new orders and orders modifications when no reservation date is set
    Given events from <scenario>
    When the streaming is executed given that <scenario>
    Then it will insert reservations info in customer orders reservations table <scenario> and trace error <error>

    Examples:
      |scenario|error|
      # One valid event and another with null reservation date
      |scenario6|Customer order reservation with picking date null:|
