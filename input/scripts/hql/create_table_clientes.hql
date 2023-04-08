CREATE EXTERNAL TABLE IF NOT EXISTS minsait.clientes ( 
        address_number string,
        business_family string,
        business_unit integer,
        customer string,
        customer_key string,
        customer_type string,
        division string,
        business_line string,
        phone string,
        region_code string,
        regional_sales string,
        search_type string
    )
COMMENT 'Tabela de Clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/clientes/'
TBLPROPERTIES ("skip.header.line.count"="1");