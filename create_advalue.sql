CREATE TABLE user_value(
   device_id CHAR(100) PRIMARY KEY NOT NULL,
   d1_ltv REAL NOT NULL,
   d2_ltv REAL NOT NULL,
   d3_ltv REAL NOT NULL,
   d4_ltv REAL NOT NULL,
   d5_ltv REAL NOT NULL,
   d6_ltv REAL NOT NULL,
   d7_ltv REAL NOT NULL,
   in_app_age INT NOT NULL,
   last_update_time DATETIME NOT NULL
);

CREATE TABLE daily_value(
	device_id CHAR(100) NOT NULL,
	ad_unit CHAR(100) NOT NULL,
	impression INT NOT NULL,
	ads_value REAL NOT NULL,
   PRIMARY KEY (device_id, ad_unit)
);

CREATE TABLE daily_new_user(
	device_id CHAR(100) PRIMARY KEY NOT NULL
);