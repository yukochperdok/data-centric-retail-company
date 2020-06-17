DROP TABLE IF EXISTS omnichannel_stream.`3b_stock_conf_oms`;
CREATE TABLE IF NOT EXISTS omnichannel_stream.`3b_stock_conf_oms`
(
    ID int NOT NULL COMMENT "Unique identification (serial)",
    MAX_NSP double NOT NULL COMMENT "Maximum value for the service levels of the supplier",
    MIN_NSP double NOT NULL COMMENT "Minimum value for the service levels of the supplier",
    MAX_NST double NOT NULL COMMENT "Maximum value for store service levels",
    MIN_NST double NOT NULL COMMENT "Minimum value for store service levels",
    ROTATION_ART_AA double COMMENT "Value of the percentage of the type of article rotation a +",
    ROTATION_ART_A double COMMENT "Value of the percentage of the type of article rotation a",
    ROTATION_ART_B double COMMENT "Value of the percentage of the type of article rotation b",
    ROTATION_ART_C double COMMENT "Value of the percentage of the type of article rotation c",
    DAY_PROJECT int NOT NULL COMMENT "Number of days to project type products",
    CREATION_DATE TIMESTAMP NOT NULL COMMENT 'control attribute. identifies the creation date.',
    CREATED_BY string COMMENT 'control attribute. identifies the user who creates the filter.',
    UPDATED_BY string COMMENT 'control attribute. identifies the user who updates the filter.',
    UPDATE_DATE TIMESTAMP COMMENT 'control attribute. identifies the last updated date.',
    TS_INSERT_DLK TIMESTAMP NOT NULL COMMENT 'Current timestamp at creation time',
    USER_INSERT_DLK string COMMENT 'User who created the entry',
    TS_UPDATE_DLK TIMESTAMP NOT NULL COMMENT 'Current timestamp at update time',
    USER_UPDATE_DLK string COMMENT 'User who updated the entry',
    PRIMARY KEY(ID)
)
PARTITION BY HASH PARTITIONS 3
COMMENT 'Configuration of days to project for calculation of stock forecast'
STORED AS KUDU
TBLPROPERTIES(
 'kudu.table_name'='omnichannel_stream.3b_stock_conf_oms'
);
