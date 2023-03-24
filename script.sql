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
#########################################################

CREATE TABLE IF NOT EXISTS desafio.cidade (
    id_cidade string,
    ds_cidade string,
    id_estado string
)
COMMENT 'Tabela de Cidade'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/cidade/'
TBLPROPERTIES ("skip.header.line.count"="1");
#########################################################

CREATE TABLE IF NOT EXISTS desafio.cliente (
    id_cliente string,
    nm_cliente string,
    flag_ouro string
)
COMMENT 'Tabela de Cliente'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/cliente/'
TBLPROPERTIES ("skip.header.line.count"="1");
#########################################################

CREATE TABLE IF NOT EXISTS desafio.estado (
    id_estado string,
    ds_estado string
)
COMMENT 'Tabela de Estado'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/estado/'
TBLPROPERTIES ("skip.header.line.count"="1");
#########################################################

CREATE TABLE IF NOT EXISTS desafio.filial (
    id_filial string,
    ds_filial string,
    id_cidade string
)
COMMENT 'Tabela de Filial'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/filial/'
TBLPROPERTIES ("skip.header.line.count"="1");
#########################################################

CREATE TABLE IF NOT EXISTS desafio.item (
    id_pedido string,
    id_produto string,
    quantidade int,
    vr_unitario string
)
COMMENT 'Tabela de Item'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/item_pedido/'
TBLPROPERTIES ("skip.header.line.count"="1");
#########################################################

CREATE TABLE IF NOT EXISTS desafio.parceiro (
    id_parceiro string,
    nm_parceiro string
)
COMMENT 'Tabela de Parceiro'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/parceiro/'
TBLPROPERTIES ("skip.header.line.count"="1");
#########################################################

CREATE TABLE IF NOT EXISTS desafio.pedido (
    id_pedido string,
    dt_pedido string,
    id_parceiro string,
    id_cliente string,
    id_filial string,
    vr_total_pago string
)
COMMENT 'Tabela de Pedido'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/pedido/'
TBLPROPERTIES ("skip.header.line.count"="1");
#########################################################

CREATE TABLE IF NOT EXISTS desafio.produto (
    id_produto string,
    ds_produto string,
    id_subcategoria string
)
COMMENT 'Tabela de Produto'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/produto/'
TBLPROPERTIES ("skip.header.line.count"="1");
#########################################################

CREATE TABLE IF NOT EXISTS desafio.subcategoria (
    id_subcategoria string,
    ds_subcategoria string,
    id_categoria string
)
COMMENT 'Tabela de Subcategoria'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/subcategoria/'
TBLPROPERTIES ("skip.header.line.count"="1");



conection string 
beeline -u jdbc:hive2://localhost:10000

select * from desafio.item limit 10;

drop table pedido;