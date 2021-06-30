-- Table: wtst.EXCHANGE_SYMBOLS_EQUITY

-- DROP TABLE wtst."EXCHANGE_SYMBOLS_EQUITY";

CREATE TABLE wtst."EXCHANGE_SYMBOLS_EQUITY"
(
    "SYMBOL" text COLLATE pg_catalog."default" NOT NULL,
    "COMPANY_NAME" text COLLATE pg_catalog."default" NOT NULL,
    "SERIES" text COLLATE pg_catalog."default" NOT NULL,
    "DT_LISTING" date NOT NULL,
    "PAIDUP_VALUE" smallint NOT NULL,
    "MARKET_LOT" smallint NOT NULL,
    "ISIN_CODE" text COLLATE pg_catalog."default" NOT NULL,
    "FACE_VALUE" smallint NOT NULL,
    CONSTRAINT "EXCHANGE_SYMBOLS_EQUITY_pkey" PRIMARY KEY ("SYMBOL")
)

TABLESPACE pg_default;

ALTER TABLE wtst."EXCHANGE_SYMBOLS_EQUITY"
    OWNER to postgres;
    
-- Table: wtst.IBKR_EOD_DATA

-- DROP TABLE wtst."IBKR_EOD_DATA";

CREATE TABLE wtst."IBKR_EOD_DATA"
(
    "IBKR_SYMBOL" text COLLATE pg_catalog."default" NOT NULL,
    "Date" timestamp without time zone NOT NULL,
    "Open" double precision NOT NULL,
    "High" double precision NOT NULL,
    "Low" double precision NOT NULL,
    "Close" double precision NOT NULL,
    "Volume" bigint NOT NULL,
    "Average" double precision NOT NULL,
    "BarCount" bigint NOT NULL,
    CONSTRAINT "IBKR_EOD_DATA_pkey" PRIMARY KEY ("IBKR_SYMBOL", "Date")
)

TABLESPACE pg_default;

ALTER TABLE wtst."IBKR_EOD_DATA"
    OWNER to postgres;

-- Table: wtst.IBKR_SYMBOLS_EQUITY

-- DROP TABLE wtst."IBKR_SYMBOLS_EQUITY";

CREATE TABLE wtst."IBKR_SYMBOLS_EQUITY"
(
    "IBKR_SYMBOL" text COLLATE pg_catalog."default" NOT NULL,
    "SYMBOL" text COLLATE pg_catalog."default" NOT NULL,
    "COMPANY_NAME" text COLLATE pg_catalog."default" NOT NULL,
    "SERIES" text COLLATE pg_catalog."default" NOT NULL,
    "DT_LISTING" date NOT NULL,
    "PAIDUP_VALUE" smallint NOT NULL,
    "MARKET_LOT" smallint NOT NULL,
    "ISIN_CODE" text COLLATE pg_catalog."default" NOT NULL,
    "FACE_VALUE" smallint NOT NULL,
    CONSTRAINT "IBKR_SYMBOLS_EQUITY_pkey" PRIMARY KEY ("IBKR_SYMBOL")
)

TABLESPACE pg_default;

ALTER TABLE wtst."IBKR_SYMBOLS_EQUITY"
    OWNER to postgres;
    
-- Table: wtst.NSE_EOD_DATA

-- DROP TABLE wtst."NSE_EOD_DATA";

CREATE TABLE wtst."NSE_EOD_DATA"
(
    "SYMBOL" text COLLATE pg_catalog."default" NOT NULL,
    "SERIES" text COLLATE pg_catalog."default" NOT NULL,
    "DATE" date NOT NULL,
    "OPEN" double precision NOT NULL,
    "HIGH" double precision NOT NULL,
    "LOW" double precision NOT NULL,
    "CLOSE" double precision NOT NULL,
    "LAST" double precision NOT NULL,
    "PREVCLOSE" double precision NOT NULL,
    "VOLUME" bigint NOT NULL,
    "TRADEVALUE" double precision NOT NULL,
    "BARCOUNT" bigint NOT NULL,
    "ISINCODE" text COLLATE pg_catalog."default" NOT NULL,
    "DUMMY" text COLLATE pg_catalog."default",
    CONSTRAINT "NSE_EOD_DATA_pkey" PRIMARY KEY ("SYMBOL", "SERIES", "DATE")
)

TABLESPACE pg_default;

ALTER TABLE wtst."NSE_EOD_DATA"
    OWNER to postgres;

-- Table: wtst.active_stocks_baseline

-- DROP TABLE wtst.active_stocks_baseline;

CREATE TABLE wtst.active_stocks_baseline
(
    symbol text COLLATE pg_catalog."default" NOT NULL,
    company_name text COLLATE pg_catalog."default" NOT NULL,
    averagetradevalue double precision NOT NULL,
    score double precision,
    CONSTRAINT "ACTIVE_STOCKS_BASELINE_pkey" PRIMARY KEY (symbol)
)

TABLESPACE pg_default;

ALTER TABLE wtst.active_stocks_baseline
    OWNER to postgres;
    
-- Table: wtst.focus_stocks

-- DROP TABLE wtst.focus_stocks;

CREATE TABLE wtst.focus_stocks
(
    ibkr_symbol text COLLATE pg_catalog."default" NOT NULL,
    company_name text COLLATE pg_catalog."default",
    averagetradevalue double precision,
    score double precision,
    CONSTRAINT focus_stocks_pkey PRIMARY KEY (ibkr_symbol)
)

TABLESPACE pg_default;

ALTER TABLE wtst.focus_stocks
    OWNER to postgres;
    
-- Table: wtst.ibkr_hist_data

-- DROP TABLE wtst.ibkr_hist_data;

CREATE TABLE wtst.ibkr_hist_data
(
    time_frame text COLLATE pg_catalog."default" NOT NULL,
    ibkr_symbol text COLLATE pg_catalog."default" NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume bigint,
    average double precision,
    barcount bigint,
    CONSTRAINT ibkr_hist_data_pkey PRIMARY KEY (time_frame, ibkr_symbol, "timestamp")
)

TABLESPACE pg_default;

ALTER TABLE wtst.ibkr_hist_data
    OWNER to postgres;
    
-- Table: wtst.ibkr_livefive_data

-- DROP TABLE wtst.ibkr_livefive_data;

CREATE TABLE wtst.ibkr_livefive_data
(
    ibkr_symbol text COLLATE pg_catalog."default",
    "timestamp" timestamp without time zone,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume bigint,
    average double precision,
    barcount bigint
)

TABLESPACE pg_default;

ALTER TABLE wtst.ibkr_livefive_data
    OWNER to postgres;

-- Table: wtst.ibkr_temp_data

-- DROP TABLE wtst.ibkr_temp_data;

CREATE TABLE wtst.ibkr_temp_data
(
    time_frame text COLLATE pg_catalog."default",
    ibkr_symbol text COLLATE pg_catalog."default",
    "timestamp" integer,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume bigint,
    average double precision,
    barcount bigint
)

TABLESPACE pg_default;

ALTER TABLE wtst.ibkr_temp_data
    OWNER to postgres;
    
-- Table: wtst.wts_process_map

-- DROP TABLE wtst.wts_process_map;

CREATE TABLE wtst.wts_process_map
(
    wts_date date NOT NULL,
    ibkr_symbol text COLLATE pg_catalog."default" NOT NULL,
    processor_index integer NOT NULL,
    processor_status text COLLATE pg_catalog."default" NOT NULL,
    processor_command text COLLATE pg_catalog."default" NOT NULL,
    ibkr_client_index integer NOT NULL,
    ibkr_req_num integer,
    ibkr_client_status text COLLATE pg_catalog."default" NOT NULL,
    ibkr_client_command text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT unique_client_keys UNIQUE (wts_date, ibkr_client_index, ibkr_req_num),
    CONSTRAINT unique_compute_keys UNIQUE (wts_date, processor_index)
)

TABLESPACE pg_default;

ALTER TABLE wtst.wts_process_map
    OWNER to postgres;
-- Index: idx_client_keys

-- DROP INDEX wtst.idx_client_keys;

CREATE INDEX idx_client_keys
    ON wtst.wts_process_map USING btree
    (wts_date DESC NULLS FIRST, ibkr_client_index ASC NULLS LAST, ibkr_req_num ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_compute_keys

-- DROP INDEX wtst.idx_compute_keys;

CREATE INDEX idx_compute_keys
    ON wtst.wts_process_map USING btree
    (wts_date DESC NULLS FIRST, processor_index ASC NULLS LAST)
    TABLESPACE pg_default;
