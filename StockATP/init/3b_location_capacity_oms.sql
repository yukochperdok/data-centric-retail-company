DROP TABLE IF EXISTS omnichannel_stream.`3b_location_capacity_oms`;
CREATE TABLE IF NOT EXISTS omnichannel_stream.`3b_location_capacity_oms`
(
    ID_STORE string NOT NULL COMMENT "Store's identification",
    ID_CAPACITY int NOT NULL COMMENT "Capacity's identification",
    CREATION_DATE TIMESTAMP NOT NULL COMMENT 'control attribute. identifies the creation date.',
    CREATED_BY string COMMENT 'control attribute. identifies the user who creates the filter.',
    UPDATED_BY string COMMENT 'control attribute. identifies the user who updates the filter.',
    UPDATE_DATE TIMESTAMP COMMENT 'control attribute. identifies the last updated date.',
    TS_INSERT_DLK TIMESTAMP NOT NULL COMMENT 'Current timestamp at creation time',
    USER_INSERT_DLK string COMMENT 'User who created the entry',
    TS_UPDATE_DLK TIMESTAMP NOT NULL COMMENT 'Current timestamp at update time',
    USER_UPDATE_DLK string COMMENT 'User who updated the entry',
    PRIMARY KEY(ID_STORE, ID_CAPACITY)
)
PARTITION BY HASH PARTITIONS 3
COMMENT 'Relationships of the capacities with each one of the stores'
STORED AS KUDU
TBLPROPERTIES(
 'kudu.table_name'='omnichannel_stream.3b_location_capacity_oms'
);
