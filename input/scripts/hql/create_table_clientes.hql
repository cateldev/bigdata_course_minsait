CREATE EXTERNAL TABLE IF NOT EXISTS minsait.clientes ( 
        address_number integer,
        business_family string,
        business_unit integer,
        customer string,
        customer_key integer,
        customer_type string,
        division integer,
        business_line string,
        phone string,
        region_code integer,
        regional_sales string,
        search_type string
    )
COMMENT 'Tabela de Clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/clientes/'
TBLPROPERTIES ("skip.header.line.count"="1");