CREATE EXTERNAL TABLE IF NOT EXISTS minsait.regiao ( 
        region_code integer,
        region_name string
    )
COMMENT 'Tabela de Regiao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/regiao/'
TBLPROPERTIES ("skip.header.line.count"="1");