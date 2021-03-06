DROP TABLE IF EXISTS omnichannel_user.`omnichannel_stock_atp`;
CREATE TABLE IF NOT EXISTS omnichannel_user.`omnichannel_stock_atp`
(
    MATNR string NOT NULL COMMENT "Article identification",
    WERKS string NOT NULL COMMENT "Store identification",
    UNIDAD_MEDIDA string COMMENT "Measure unit",
    SECTOR string COMMENT "Sector",
    IS_PP boolean NOT NULL COMMENT "If the store has the capacity of preparation",
    N_DATE timestamp NOT NULL COMMENT "The day of N",
    TS_INSERT_DLK timestamp COMMENT "Inserted date",
    USER_INSERT_DLK string COMMENT "User that inserted the registry",
    TS_UPDATE_DLK timestamp NOT NULL COMMENT "Updated date",
    USER_UPDATE_DLK string COMMENT "User that updated the registry",
    STOCK_ATP_N double NULL COMMENT "Stock ATP for day N",
    STOCK_ATP_N1 double NULL COMMENT "Stock ATP for day N + 1",
    STOCK_ATP_N2 double NULL COMMENT "Stock ATP for day N + 2",
    STOCK_ATP_N3 double NULL COMMENT "Stock ATP for day N + 3",
    STOCK_ATP_N4 double NULL COMMENT "Stock ATP for day N + 4",
    STOCK_ATP_N5 double NULL COMMENT "Stock ATP for day N + 5",
    STOCK_ATP_N6 double NULL COMMENT "Stock ATP for day N + 6",
    STOCK_ATP_N7 double NULL COMMENT "Stock ATP for day N + 7",
    STOCK_ATP_N8 double NULL COMMENT "Stock ATP for day N + 8",
    STOCK_ATP_N9 double NULL COMMENT "Stock ATP for day N + 9",
    STOCK_ATP_N10 double NULL COMMENT "Stock ATP for day N + 10",
    STOCK_ATP_N11 double NULL COMMENT "Stock ATP for day N + 11",
    STOCK_ATP_N12 double NULL COMMENT "Stock ATP for day N + 12",
    STOCK_ATP_N13 double NULL COMMENT "Stock ATP for day N + 13",
    STOCK_ATP_N14 double NULL COMMENT "Stock ATP for day N + 14",
    STOCK_ATP_N15 double NULL COMMENT "Stock ATP for day N + 15",
    STOCK_ATP_N16 double NULL COMMENT "Stock ATP for day N + 16",
    STOCK_ATP_N17 double NULL COMMENT "Stock ATP for day N + 17",
    STOCK_ATP_N18 double NULL COMMENT "Stock ATP for day N + 18",
    STOCK_ATP_N19 double NULL COMMENT "Stock ATP for day N + 19",
    STOCK_ATP_N20 double NULL COMMENT "Stock ATP for day N + 20",
    STOCK_ATP_N21 double NULL COMMENT "Stock ATP for day N + 21",
    STOCK_ATP_N22 double NULL COMMENT "Stock ATP for day N + 22",
    STOCK_ATP_N23 double NULL COMMENT "Stock ATP for day N + 23",
    STOCK_ATP_N24 double NULL COMMENT "Stock ATP for day N + 24",
    STOCK_ATP_N25 double NULL COMMENT "Stock ATP for day N + 25",
    STOCK_ATP_N26 double NULL COMMENT "Stock ATP for day N + 26",
    STOCK_ATP_N27 double NULL COMMENT "Stock ATP for day N + 27",
    STOCK_ATP_N28 double NULL COMMENT "Stock ATP for day N + 28",
    STOCK_ATP_N29 double NULL COMMENT "Stock ATP for day N + 29",
    STOCK_ATP_N30 double NULL COMMENT "Stock ATP for day N + 30",
    STOCK_ATP_N31 double NULL COMMENT "Stock ATP for day N + 31",
    STOCK_ATP_N32 double NULL COMMENT "Stock ATP for day N + 32",
    STOCK_ATP_N33 double NULL COMMENT "Stock ATP for day N + 33",
    STOCK_ATP_N34 double NULL COMMENT "Stock ATP for day N + 34",
    STOCK_ATP_N35 double NULL COMMENT "Stock ATP for day N + 35",
    STOCK_ATP_N36 double NULL COMMENT "Stock ATP for day N + 36",
    STOCK_ATP_N37 double NULL COMMENT "Stock ATP for day N + 37",
    STOCK_ATP_N38 double NULL COMMENT "Stock ATP for day N + 38",
    STOCK_ATP_N39 double NULL COMMENT "Stock ATP for day N + 39",
    STOCK_ATP_N40 double NULL COMMENT "Stock ATP for day N + 40",
    STOCK_ATP_N41 double NULL COMMENT "Stock ATP for day N + 41",
    STOCK_ATP_N42 double NULL COMMENT "Stock ATP for day N + 42",
    STOCK_ATP_N43 double NULL COMMENT "Stock ATP for day N + 43",
    STOCK_ATP_N44 double NULL COMMENT "Stock ATP for day N + 44",
    STOCK_ATP_N45 double NULL COMMENT "Stock ATP for day N + 45",
    STOCK_ATP_N46 double NULL COMMENT "Stock ATP for day N + 46",
    STOCK_ATP_N47 double NULL COMMENT "Stock ATP for day N + 47",
    STOCK_ATP_N48 double NULL COMMENT "Stock ATP for day N + 48",
    STOCK_ATP_N49 double NULL COMMENT "Stock ATP for day N + 49",
    STOCK_ATP_N50 double NULL COMMENT "Stock ATP for day N + 50",
    STOCK_ATP_N51 double NULL COMMENT "Stock ATP for day N + 51",
    STOCK_ATP_N52 double NULL COMMENT "Stock ATP for day N + 52",
    STOCK_ATP_N53 double NULL COMMENT "Stock ATP for day N + 53",
    STOCK_ATP_N54 double NULL COMMENT "Stock ATP for day N + 54",
    STOCK_ATP_N55 double NULL COMMENT "Stock ATP for day N + 55",
    STOCK_ATP_N56 double NULL COMMENT "Stock ATP for day N + 56",
    STOCK_ATP_N57 double NULL COMMENT "Stock ATP for day N + 57",
    STOCK_ATP_N58 double NULL COMMENT "Stock ATP for day N + 58",
    STOCK_ATP_N59 double NULL COMMENT "Stock ATP for day N + 59",
    STOCK_ATP_N60 double NULL COMMENT "Stock ATP for day N + 60",
    STOCK_ATP_N61 double NULL COMMENT "Stock ATP for day N + 61",
    STOCK_ATP_N62 double NULL COMMENT "Stock ATP for day N + 62",
    STOCK_ATP_N63 double NULL COMMENT "Stock ATP for day N + 63",
    STOCK_ATP_N64 double NULL COMMENT "Stock ATP for day N + 64",
    STOCK_ATP_N65 double NULL COMMENT "Stock ATP for day N + 65",
    STOCK_ATP_N66 double NULL COMMENT "Stock ATP for day N + 66",
    STOCK_ATP_N67 double NULL COMMENT "Stock ATP for day N + 67",
    STOCK_ATP_N68 double NULL COMMENT "Stock ATP for day N + 68",
    STOCK_ATP_N69 double NULL COMMENT "Stock ATP for day N + 69",
    STOCK_ATP_N70 double NULL COMMENT "Stock ATP for day N + 70",
    STOCK_ATP_N71 double NULL COMMENT "Stock ATP for day N + 71",
    STOCK_ATP_N72 double NULL COMMENT "Stock ATP for day N + 72",
    STOCK_ATP_N73 double NULL COMMENT "Stock ATP for day N + 73",
    STOCK_ATP_N74 double NULL COMMENT "Stock ATP for day N + 74",
    STOCK_ATP_N75 double NULL COMMENT "Stock ATP for day N + 75",
    STOCK_ATP_N76 double NULL COMMENT "Stock ATP for day N + 76",
    STOCK_ATP_N77 double NULL COMMENT "Stock ATP for day N + 77",
    STOCK_ATP_N78 double NULL COMMENT "Stock ATP for day N + 78",
    STOCK_ATP_N79 double NULL COMMENT "Stock ATP for day N + 79",
    STOCK_ATP_N80 double NULL COMMENT "Stock ATP for day N + 80",
    STOCK_ATP_N81 double NULL COMMENT "Stock ATP for day N + 81",
    STOCK_ATP_N82 double NULL COMMENT "Stock ATP for day N + 82",
    STOCK_ATP_N83 double NULL COMMENT "Stock ATP for day N + 83",
    STOCK_ATP_N84 double NULL COMMENT "Stock ATP for day N + 84",
    STOCK_ATP_N85 double NULL COMMENT "Stock ATP for day N + 85",
    STOCK_ATP_N86 double NULL COMMENT "Stock ATP for day N + 86",
    STOCK_ATP_N87 double NULL COMMENT "Stock ATP for day N + 87",
    STOCK_ATP_N88 double NULL COMMENT "Stock ATP for day N + 88",
    STOCK_ATP_N89 double NULL COMMENT "Stock ATP for day N + 89",
    STOCK_ATP_N90 double NULL COMMENT "Stock ATP for day N + 90",
    STOCK_ATP_N91 double NULL COMMENT "Stock ATP for day N + 91",
    STOCK_ATP_N92 double NULL COMMENT "Stock ATP for day N + 92",
    STOCK_ATP_N93 double NULL COMMENT "Stock ATP for day N + 93",
    STOCK_ATP_N94 double NULL COMMENT "Stock ATP for day N + 94",
    STOCK_ATP_N95 double NULL COMMENT "Stock ATP for day N + 95",
    STOCK_ATP_N96 double NULL COMMENT "Stock ATP for day N + 96",
    STOCK_ATP_N97 double NULL COMMENT "Stock ATP for day N + 97",
    STOCK_ATP_N98 double NULL COMMENT "Stock ATP for day N + 98",
    STOCK_ATP_N99 double NULL COMMENT "Stock ATP for day N + 99",
    STOCK_ATP_N100 double NULL COMMENT "Stock ATP for day N + 100",
    STOCK_ATP_N101 double NULL COMMENT "Stock ATP for day N + 101",
    PRIMARY KEY(MATNR, WERKS)
)
PARTITION BY HASH(MATNR, WERKS) PARTITIONS 10
COMMENT 'Available Stock in stores'
STORED AS KUDU;

