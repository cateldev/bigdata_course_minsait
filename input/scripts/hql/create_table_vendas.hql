CREATE EXTERNAL TABLE IF NOT EXISTS minsait.vendas ( 
        dt_delivery string,
        customer_key string,
        dt_key string,
        discount_amount string,
        dt_invoice string,
        invoice_number string,
        item_class string,
        item_number string,
        item string,
        line_number string,
        price_list string,
        order_number string,
        dt_promised string,
        total_sales string,
        list_price_sales string,
        total_sales_cost string,
        total_sales_margin string,
        sales_price string,
        sales_quantity string,
        sales_rep string,
        u_m string
    )
COMMENT 'Tabela de Vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/vendas/'
TBLPROPERTIES ("skip.header.line.count"="1");