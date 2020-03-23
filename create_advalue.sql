CREATE TABLE IF NOT EXISTS user_value(
   device_id CHAR(100) PRIMARY KEY NOT NULL,
   d1_ltv REAL,
   d2_ltv REAL,
   d3_ltv REAL,
   d4_ltv REAL,
   d5_ltv REAL,
   d6_ltv REAL,
   d7_ltv REAL,
   in_app_age INT NOT NULL,
   last_update_time DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_value(
	device_id CHAR(100) NOT NULL,
	ad_unit CHAR(100) NOT NULL,
	impression INT NOT NULL,
	ads_value REAL NOT NULL,
   PRIMARY KEY (device_id, ad_unit)
);

CREATE TABLE IF NOT EXISTS daily_new_user(
	device_id CHAR(100) PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_operation_record(
    op_date DATETIME PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS value_threshold(
    d1_threshold REAL,
    d2_threshold REAL,
    d3_threshold REAL,
    d4_threshold REAL,
    d5_threshold REAL,
    d6_threshold REAL,
    d7_threshold REAL
)