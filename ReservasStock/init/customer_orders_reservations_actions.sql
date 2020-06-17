DROP TABLE IF EXISTS omnichannel_user.`customer_orders_reservations_actions`;
CREATE TABLE IF NOT EXISTS omnichannel_user.`customer_orders_reservations_actions`
(
    site_id             STRING       NOT NULL COMMENT "Order origin",
    order_status        STRING       NOT NULL COMMENT "Origin order status",
    sherpa_order_status STRING       NOT NULL COMMENT "Sherpa Order Status equivalent to origin order status",
    action              STRING       NOT NULL COMMENT "Action to perforn for each origin order status",
    ts_insert_dlk       TIMESTAMP    NOT NULL COMMENT "Inserted date",
    user_insert_dlk     STRING       NOT NULL COMMENT "User that inserted the registry",
    ts_update_dlk       TIMESTAMP    NULL     COMMENT "Updated date",
    user_update_dlk     STRING       NULL     COMMENT "User that updated the registry",
    PRIMARY KEY(site_id, order_status)
)
COMMENT 'Actions to perform for each origin of the orders'
STORED AS KUDU;
