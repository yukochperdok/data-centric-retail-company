DROP TABLE IF EXISTS omnichannel_user.`omnichannel_datos_fijos`;
CREATE EXTERNAL TABLE omnichannel_user.`omnichannel_datos_fijos`
(
    MATNR string COMMENT "Article identification",
    WERKS string COMMENT "Store identification",
    DATE_SALE string COMMENT "Sale date",
    SECTOR string COMMENT "Sector",
    UNIDAD_MEDIDA string COMMENT "Unidad medida",
    NSP double COMMENT "Supplier service level",
    NST double COMMENT "Store service level",
    ROTATION string COMMENT "Rotation type",
    TS_INSERT_DLK timestamp COMMENT "Inserted date",
    USER_INSERT_DLK string COMMENT "User that inserted the registry",
    TS_UPDATE_DLK timestamp COMMENT "Updated date",
    USER_UPDATE_DLK string COMMENT "User that updated the registry",
    SALES_FORECAST_N double COMMENT "Sales forecast for day N",
    SALES_FORECAST_N1 double COMMENT "Sales forecast for day N + 1",
    SALES_FORECAST_N2 double COMMENT "Sales forecast for day N + 2",
    SALES_FORECAST_N3 double COMMENT "Sales forecast for day N + 3",
    SALES_FORECAST_N4 double COMMENT "Sales forecast for day N + 4",
    SALES_FORECAST_N5 double COMMENT "Sales forecast for day N + 5",
    SALES_FORECAST_N6 double COMMENT "Sales forecast for day N + 6",
    SALES_FORECAST_N7 double COMMENT "Sales forecast for day N + 7",
    SALES_FORECAST_N8 double COMMENT "Sales forecast for day N + 8",
    SALES_FORECAST_N9 double COMMENT "Sales forecast for day N + 9",
    SALES_FORECAST_N10 double COMMENT "Sales forecast for day N + 10",
    SALES_FORECAST_N11 double COMMENT "Sales forecast for day N + 11",
    SALES_FORECAST_N12 double COMMENT "Sales forecast for day N + 12",
    SALES_FORECAST_N13 double COMMENT "Sales forecast for day N + 13",
    SALES_FORECAST_N14 double COMMENT "Sales forecast for day N + 14",
    SALES_FORECAST_N15 double COMMENT "Sales forecast for day N + 15",
    SALES_FORECAST_N16 double COMMENT "Sales forecast for day N + 16",
    SALES_FORECAST_N17 double COMMENT "Sales forecast for day N + 17",
    SALES_FORECAST_N18 double COMMENT "Sales forecast for day N + 18",
    SALES_FORECAST_N19 double COMMENT "Sales forecast for day N + 19",
    SALES_FORECAST_N20 double COMMENT "Sales forecast for day N + 20",
    SALES_FORECAST_N21 double COMMENT "Sales forecast for day N + 21",
    SALES_FORECAST_N22 double COMMENT "Sales forecast for day N + 22",
    SALES_FORECAST_N23 double COMMENT "Sales forecast for day N + 23",
    SALES_FORECAST_N24 double COMMENT "Sales forecast for day N + 24",
    SALES_FORECAST_N25 double COMMENT "Sales forecast for day N + 25",
    SALES_FORECAST_N26 double COMMENT "Sales forecast for day N + 26",
    SALES_FORECAST_N27 double COMMENT "Sales forecast for day N + 27",
    SALES_FORECAST_N28 double COMMENT "Sales forecast for day N + 28",
    SALES_FORECAST_N29 double COMMENT "Sales forecast for day N + 29",
    SALES_FORECAST_N30 double COMMENT "Sales forecast for day N + 30",
    SALES_FORECAST_N31 double COMMENT "Sales forecast for day N + 31",
    SALES_FORECAST_N32 double COMMENT "Sales forecast for day N + 32",
    SALES_FORECAST_N33 double COMMENT "Sales forecast for day N + 33",
    SALES_FORECAST_N34 double COMMENT "Sales forecast for day N + 34",
    SALES_FORECAST_N35 double COMMENT "Sales forecast for day N + 35",
    SALES_FORECAST_N36 double COMMENT "Sales forecast for day N + 36",
    SALES_FORECAST_N37 double COMMENT "Sales forecast for day N + 37",
    SALES_FORECAST_N38 double COMMENT "Sales forecast for day N + 38",
    SALES_FORECAST_N39 double COMMENT "Sales forecast for day N + 39",
    SALES_FORECAST_N40 double COMMENT "Sales forecast for day N + 40",
    SALES_FORECAST_N41 double COMMENT "Sales forecast for day N + 41",
    SALES_FORECAST_N42 double COMMENT "Sales forecast for day N + 42",
    SALES_FORECAST_N43 double COMMENT "Sales forecast for day N + 43",
    SALES_FORECAST_N44 double COMMENT "Sales forecast for day N + 44",
    SALES_FORECAST_N45 double COMMENT "Sales forecast for day N + 45",
    SALES_FORECAST_N46 double COMMENT "Sales forecast for day N + 46",
    SALES_FORECAST_N47 double COMMENT "Sales forecast for day N + 47",
    SALES_FORECAST_N48 double COMMENT "Sales forecast for day N + 48",
    SALES_FORECAST_N49 double COMMENT "Sales forecast for day N + 49",
    SALES_FORECAST_N50 double COMMENT "Sales forecast for day N + 50",
    SALES_FORECAST_N51 double COMMENT "Sales forecast for day N + 51",
    SALES_FORECAST_N52 double COMMENT "Sales forecast for day N + 52",
    SALES_FORECAST_N53 double COMMENT "Sales forecast for day N + 53",
    SALES_FORECAST_N54 double COMMENT "Sales forecast for day N + 54",
    SALES_FORECAST_N55 double COMMENT "Sales forecast for day N + 55",
    SALES_FORECAST_N56 double COMMENT "Sales forecast for day N + 56",
    SALES_FORECAST_N57 double COMMENT "Sales forecast for day N + 57",
    SALES_FORECAST_N58 double COMMENT "Sales forecast for day N + 58",
    SALES_FORECAST_N59 double COMMENT "Sales forecast for day N + 59",
    SALES_FORECAST_N60 double COMMENT "Sales forecast for day N + 60",
    SALES_FORECAST_N61 double COMMENT "Sales forecast for day N + 61",
    SALES_FORECAST_N62 double COMMENT "Sales forecast for day N + 62",
    SALES_FORECAST_N63 double COMMENT "Sales forecast for day N + 63",
    SALES_FORECAST_N64 double COMMENT "Sales forecast for day N + 64",
    SALES_FORECAST_N65 double COMMENT "Sales forecast for day N + 65",
    SALES_FORECAST_N66 double COMMENT "Sales forecast for day N + 66",
    SALES_FORECAST_N67 double COMMENT "Sales forecast for day N + 67",
    SALES_FORECAST_N68 double COMMENT "Sales forecast for day N + 68",
    SALES_FORECAST_N69 double COMMENT "Sales forecast for day N + 69",
    SALES_FORECAST_N70 double COMMENT "Sales forecast for day N + 70",
    SALES_FORECAST_N71 double COMMENT "Sales forecast for day N + 71",
    SALES_FORECAST_N72 double COMMENT "Sales forecast for day N + 72",
    SALES_FORECAST_N73 double COMMENT "Sales forecast for day N + 73",
    SALES_FORECAST_N74 double COMMENT "Sales forecast for day N + 74",
    SALES_FORECAST_N75 double COMMENT "Sales forecast for day N + 75",
    SALES_FORECAST_N76 double COMMENT "Sales forecast for day N + 76",
    SALES_FORECAST_N77 double COMMENT "Sales forecast for day N + 77",
    SALES_FORECAST_N78 double COMMENT "Sales forecast for day N + 78",
    SALES_FORECAST_N79 double COMMENT "Sales forecast for day N + 79",
    SALES_FORECAST_N80 double COMMENT "Sales forecast for day N + 80",
    SALES_FORECAST_N81 double COMMENT "Sales forecast for day N + 81",
    SALES_FORECAST_N82 double COMMENT "Sales forecast for day N + 82",
    SALES_FORECAST_N83 double COMMENT "Sales forecast for day N + 83",
    SALES_FORECAST_N84 double COMMENT "Sales forecast for day N + 84",
    SALES_FORECAST_N85 double COMMENT "Sales forecast for day N + 85",
    SALES_FORECAST_N86 double COMMENT "Sales forecast for day N + 86",
    SALES_FORECAST_N87 double COMMENT "Sales forecast for day N + 87",
    SALES_FORECAST_N88 double COMMENT "Sales forecast for day N + 88",
    SALES_FORECAST_N89 double COMMENT "Sales forecast for day N + 89",
    SALES_FORECAST_N90 double COMMENT "Sales forecast for day N + 90",
    SALES_FORECAST_N91 double COMMENT "Sales forecast for day N + 91",
    SALES_FORECAST_N92 double COMMENT "Sales forecast for day N + 92",
    SALES_FORECAST_N93 double COMMENT "Sales forecast for day N + 93",
    SALES_FORECAST_N94 double COMMENT "Sales forecast for day N + 94",
    SALES_FORECAST_N95 double COMMENT "Sales forecast for day N + 95",
    SALES_FORECAST_N96 double COMMENT "Sales forecast for day N + 96",
    SALES_FORECAST_N97 double COMMENT "Sales forecast for day N + 97",
    SALES_FORECAST_N98 double COMMENT "Sales forecast for day N + 98",
    SALES_FORECAST_N99 double COMMENT "Sales forecast for day N + 99",
    SALES_FORECAST_N100 double COMMENT "Sales forecast for day N + 100",
    SALES_FORECAST_N101 double COMMENT "Sales forecast for day N + 101",
    SALES_FORECAST_N102 double COMMENT "Sales forecast for day N + 102"
)
COMMENT 'Fixed data forecast'
STORED AS PARQUET;