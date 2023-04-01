CREATE EXTERNAL TABLE IF NOT EXISTS minsait.vendas ( 
        dt_delivery date,
        customer_key integer,
        dt_key date,
        discount_amount float,
        dt_invoice date,
        invoice_number integer,
        item_class string,
        item_number integer,
        item string,
        line_number integer,
        price_list float,
        order_number integer,
        dt_promised date,
        total_sales float,
        list_price_sales float,
        total_sales_cost float,
        total_sales_margin float,
        sales_price float,
        sales_quantity integer,
        sales_rep integer,
        u_m string
    )
COMMENT 'Tabela de Vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/vendas/'
TBLPROPERTIES ("skip.header.line.count"="1");