CREATE TABLE IF NOT EXISTS  daily_value(
	device_id CHAR(100) NOT NULL,
	ad_unit CHAR(100) NOT NULL,
	impression INT NOT NULL,
	ads_value REAL NOT NULL,
   PRIMARY KEY (device_id, ad_unit)
);