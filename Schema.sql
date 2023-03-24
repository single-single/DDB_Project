DROP TABLE IF EXISTS warehouse CASCADE;
DROP TABLE IF EXISTS district CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS item CASCADE;
DROP TABLE IF EXISTS orderLine CASCADE;
DROP TABLE IF EXISTS stock CASCADE;

CREATE TABLE IF NOT EXISTS warehouse (
	W_ID int,
	W_NAME varchar(25),
	W_STREET_1 varchar(25),
	W_STREET_2 varchar(25),
	W_CITY varchar(25),
	W_STATE char(2),
	W_ZIP char(9),
	W_TAX decimal(4,4),
	W_YTD decimal(12,2),
	PRIMARY KEY (W_ID)
);


CREATE TABLE IF NOT EXISTS item (
    I_ID int,
    I_NAME varchar(24),
    I_PRICE decimal(5,2),
    I_IM_ID int,
    I_DATA varchar(50),
    PRIMARY KEY(I_ID)
);

CREATE TABLE IF NOT EXISTS stock (
    S_W_ID       int       NOT NULL REFERENCES warehouse (W_ID),
    S_I_ID       int       NOT NULL REFERENCES item (I_ID),
    S_QUANTITY   int  NOT NULL DEFAULT 0,
    S_YTD        decimal(8,2)  NOT NULL DEFAULT 0.00,
    S_ORDER_CNT  int       NOT NULL DEFAULT 0,
    S_REMOTE_CNT int       NOT NULL DEFAULT 0,
    S_DIST_01    varchar(25),
    S_DIST_02    varchar(25),
    S_DIST_03    varchar(25),
    S_DIST_04    varchar(25),
    S_DIST_05    varchar(25),
    S_DIST_06    varchar(25),
    S_DIST_07    varchar(25),
    S_DIST_08    varchar(25),
    S_DIST_09    varchar(25),
    S_DIST_10    varchar(25),
    S_DATA       varchar(50),
    PRIMARY KEY  (S_W_ID, S_I_ID)
);

CREATE TABLE IF NOT EXISTS district (
	D_W_ID int,
	D_ID int,
	D_NAME varchar(10),
	D_STREET_1 varchar(25),
	D_STREET_2 varchar(25),
	D_CITY varchar(25),
	D_STATE char(2),
	D_ZIP char(9),
	D_TAX decimal(4,4),
	D_YTD decimal(12,2),
	D_NEXT_O_ID int,
	PRIMARY KEY (D_W_ID, D_ID),
    CONSTRAINT fk_district_warehouse_id
    FOREIGN KEY (D_W_ID) REFERENCES warehouse(W_ID)
);

CREATE TABLE IF NOT EXISTS customer (
	C_W_ID int,
	C_D_ID int,
	C_ID int,
	C_FIRST varchar(16),
	C_MIDDLE char(2),
	C_LAST varchar(16),
	C_STREET_1 varchar(25),
	C_STREET_2 varchar(25),
	C_CITY varchar(25),
	C_STATE char(2),
	C_ZIP char(9),
	C_PHONE char(16),
	C_SINCE timestamp,
	C_CREDIT char(2),
	C_CREDIT_LIM decimal(12,2),
	C_DISCOUNT decimal(5,4),
	C_BALANCE decimal(12, 2),
	C_YTD_PAYMENT float,
	C_PAYMENT_CNT int,
	C_DELIVERY_CNT int,
	C_DATA varchar(500),
	PRIMARY KEY(C_W_ID, C_D_ID, C_ID),
    CONSTRAINT fk_warehouse_district_id
    FOREIGN KEY (C_W_ID, C_D_ID) REFERENCES district(D_W_ID, D_ID)
);

CREATE TABLE IF NOT EXISTS orders (
    O_W_ID int NOT NULL,
    O_D_ID int NOT NULL,
    O_ID int NOT NULL,
    O_C_ID int NOT NULL,
    O_CARRIER_ID char(9),
    O_OL_CNT decimal(2,0),
    O_ALL_LOCAL decimal(1,0),
    O_ENTRY_D timestamp,
    PRIMARY KEY(O_W_ID, O_D_ID, O_ID),
    CONSTRAINT fk_warehouse_district_customer_id
    FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES customer(C_W_ID, C_D_ID, C_ID)
);




CREATE TABLE IF NOT EXISTS orderLine (
	OL_W_ID int,
	OL_D_ID int,
	OL_O_ID int,
	OL_NUMBER int,
	OL_I_ID int,
	OL_DELIVERY_D timestamp,
	OL_AMOUNT decimal(7, 2),
	OL_SUPPLY_W_ID int,
	OL_QUANTITY decimal(2, 0),
	OL_DIST_INFO char(24),
	PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER),
    CONSTRAINT fk_warehouse_district_order_id
    FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES orders(O_W_ID, O_D_ID, O_C_ID),
    CONSTRAINT fk_item_id
    FOREIGN KEY (OL_I_ID) REFERENCES item(I_ID)
);

