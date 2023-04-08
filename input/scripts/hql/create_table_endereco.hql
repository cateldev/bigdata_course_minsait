CREATE EXTERNAL TABLE IF NOT EXISTS minsait.endereco ( 
        address_number string,
        city string,
        country string,
        customer_address string,
        customer_address_2 string,
        customer_address_3 string,
        customer_address_4 string,
        state string,
        zip_code string
    )
COMMENT 'Tabela de Endereco'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/endereco/'
TBLPROPERTIES ("skip.header.line.count"="1");