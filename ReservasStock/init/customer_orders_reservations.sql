DROP TABLE IF EXISTS omnichannel_stream.`customer_orders_reservations`;
CREATE TABLE IF NOT EXISTS omnichannel_stream.`customer_orders_reservations`
(
    MATNR                  STRING    NOT NULL COMMENT "Article Id",
    WERKS                  STRING    NOT NULL COMMENT "Store Id",
    ORDER_ID               STRING    NOT NULL COMMENT "Order Id",
    SITE_ID                STRING    NOT NULL COMMENT "Order Origin",
    ORDER_DATE             TIMESTAMP          COMMENT "Order Date",
    ORDER_PREPARATION_DATE TIMESTAMP          COMMENT "Order Preparation date",
    ORDER_DELIVERY_DATE    TIMESTAMP          COMMENT "Order Delivery date",
    UNIT_MEASURE           STRING             COMMENT "Unit measure",
    AMOUNT                 DOUBLE             COMMENT "Amount of stock reserved in that order",
    SHERPA_ORDER_STATUS    STRING    NOT NULL COMMENT "Sherpa Order Status equivalent to origin order status",
    TS_INSERT_DLK          TIMESTAMP NOT NULL COMMENT "Inserted date",
    USER_INSERT_DLK        STRING    NOT NULL COMMENT "User that inserted the registry",
    TS_UPDATE_DLK          TIMESTAMP NOT NULL COMMENT "Updated date",
    USER_UPDATE_DLK        STRING    NOT NULL COMMENT "User that updated the registry",
    PRIMARY KEY(MATNR, WERKS, ORDER_ID, SITE_ID)
)
PARTITION BY HASH(MATNR) PARTITIONS 3
COMMENT 'Customer orders reservations'
STORED AS KUDU;