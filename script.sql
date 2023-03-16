CREATE TABLE IF NOT EXISTS desafio.categorias (
    id_categoria string,
    ds_categoria string,
    perc_parceiro string
)
COMMENT 'Tabela de Categorias'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/categorias/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS desafio.cidade (
    id_cidade string,
    ds_cidade string,
    perc_parceiro string
)
COMMENT 'Tabela de Cidade'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/cidade/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS desafio.cliente (
    id_cliente string,
    ds_cliente string,
    perc_parceiro string
)
COMMENT 'Tabela de Cliente'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/cliente/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS desafio.estado (
    id_estado string,
    ds_estado string,
    perc_parceiro string
)
COMMENT 'Tabela de Estado'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/estado/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS desafio.filial (
    id_filial string,
    ds_filial string,
    perc_parceiro string
)
COMMENT 'Tabela de Filial'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/filial/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS desafio.item (
    id_item string,
    ds_item string,
    perc_parceiro string
)
COMMENT 'Tabela de Item'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/item/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS desafio.parceiro (
    id_parceiro string,
    ds_parceiro string,
    perc_parceiro string
)
COMMENT 'Tabela de Parceiro'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/parceiro/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS desafio.pedido (
    id_pedido string,
    ds_pedido string,
    perc_parceiro string
)
COMMENT 'Tabela de Pedido'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/pedido/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS desafio.produto (
    id_produto string,
    ds_produto string,
    perc_parceiro string
)
COMMENT 'Tabela de Produto'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/produto/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS desafio.subcategoria (
    id_subcategoria string,
    ds_subcategoria string,
    perc_parceiro string
)
COMMENT 'Tabela de Subcategoria'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/subcategoria/'
TBLPROPERTIES ("skip.header.line.count"="1");
