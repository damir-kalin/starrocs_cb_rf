CREATE TABLE stg.exchange_rates(
	id bigint NOT NULL AUTO_INCREMENT COMMENT "ID",
	n int COMMENT "Number row in file",
	value string COMMENT "Data about rates",
	dt_value date COMMENT "Date parse",
	dt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT "Date and time download"
)
ENGINE=OLAP
PRIMARY KEY(id)
DISTRIBUTED BY HASH(id);