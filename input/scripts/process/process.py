## Fazendo as importações necessárias ##
import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, substring
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import when, col, sum, count, isnan, round
from pyspark.sql.functions import regexp_replace, concat_ws, sha2, rtrim, substring
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext

 
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when
 
spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

## Salvar os dados no hive ##

def salvar_df(df, file):
    output = "/input/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
    .format("csv")\
    .option("header", True)\
    .option("delimiter", ";")\
    .mode("overwrite")\
    .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

## Criando os dataframes das tabelas do database minsait ##

df_vendas = spark.sql("select * from minsait.vendas")
df_clientes = spark.sql("select * from minsait.clientes")
df_divisao = spark.sql("select * from minsait.divisao")
df_endereco = spark.sql("select * from minsait.endereco")
df_regiao = spark.sql("select * from minsait.regiao")

## Criando a tabela stage de vendas ##

df_vendas.createOrReplaceTempView("TBL_VENDAS")

df_stage_vendas = spark.sql (''' 
        SELECT
            customer_key, 
            dt_key, 
            item, 
            total_sales, 
            sales_quantity, 
            total_sales_margin,
            sales_rep
        FROM
            TBL_VENDAS
''')

df_stage_vendas.show(10)

## Criando a tabela stage de vendas ##

df_clientes.createOrReplaceTempView("TBL_CLIENTES")

df_stage_clientes = spark.sql (''' 
        SELECT
            address_number,
            customer_key,
            customer_type,
            phone,
            division,
            region_code 
        FROM
            TBL_CLIENTES
''')

## Criando tabela stage clientes_regiao ##

df_stage_clientes_regiao = df_stage_clientes.join(df_regiao,df_clientes.region_code ==  df_regiao.region_code,"inner")\
.drop(df_stage_clientes.region_code)


## Criando a stage compilada ##

df_endereco.createOrReplaceTempView("TBL_ENDERECO")

df_stage_end = spark.sql (''' 
        SELECT
            address_number,
            city,
            country,
            state,
            zip_code
        FROM
            TBL_ENDERECO
''')


## JOIN clientes e regiao ##

df_stage_clientes_final =\
df_stage_clientes_regiao.join\
(df_stage_end, df_stage_clientes_regiao.address_number == df_stage_end.address_number,"left")\
.drop(df_stage_clientes_regiao.address_number)

## Join entre as tabelas Clientes ##

df_todos = df_stage_clientes_final.join(df_stage_vendas, df_stage_clientes_final.customer_key == df_stage_vendas.customer_key, "left").drop(df_stage_vendas.customer_key)

## Criando coluna de datas ##

df_stage_final = df_todos.select('*',substring('dt_key', 1,2 ).alias('Dia'),\
                                        substring('dt_key', 4,2 ).alias('Mes'),\
                                        substring('dt_key', 7,4 ).alias('Ano')).drop(df_todos.dt_key)


## Trocando os valores NULL

df_stage = df_stage_final.na.fill("Não informado")

## Criando a coluna PK_CLIENTE ##

df_stage = df_stage.withColumn('PK_CLIENTE', sha2(concat_ws("",df_stage.customer_key, df_stage.customer_type, df_stage.phone), 256))

df_stage.show(5, truncate=False)

## Criando PK_LOCALIDADE ##

df_stage = df_stage.withColumn('PK_LOCALIDADE', sha2(concat_ws("",df_stage.region_code, df_stage.region_name, df_stage.city, df_stage.country, df_stage.state, df_stage.zip_code), 256))

df_stage.show(5, truncate=False)

## Criando a PK_TEMPO ##

df_stage = df_stage.withColumn('PK_TEMPO', sha2(concat_ws("",df_stage.Dia, df_stage.Mes, df_stage.Ano), 256))

df_stage.show(5, truncate=False)

## Criando a tabela Temporaria para Stage ##

df_stage.createOrReplaceTempView("STAGE")

## Criando a FT_VENDAS ##

ft_vendas = spark.sql('''
    SELECT 
        PK_CLIENTE, 
        PK_LOCALIDADE, 
        PK_TEMPO, 
        item as ITEMS, 
        sales_quantity AS QUANTIDADE_VENDAS, 
        total_sales AS VALOR_VENDA
    FROM
        STAGE
''')

## Criando a DIM_CLIENTES ##

df_dim_clientes = spark.sql ('''
    SELECT DISTINCT
        PK_CLIENTE,
        customer_key AS ID_CLIENTE,
        customer_type AS TIPO_CLIENTE,
        phone AS TELEFONE
    FROM 
        STAGE
''')

## Criando a DIM_TEMPO ##

df_dim_tempo = spark.sql ('''
    SELECT DISTINCT
        PK_TEMPO,
        Dia AS DIA,
        Mes AS MES,
        Ano AS ANO
    FROM 
        STAGE
''')

## Criando a DIM_LOCALIDADE ##

df_dim_localidade = spark.sql ('''
    SELECT DISTINCT
        PK_LOCALIDADE,
        region_code AS CODIGO_REGIAO,
        region_name AS _NOME_REGIAO,
        city AS CIDADE,
        country AS PAIS,
        state AS ESTADO,
        zip_code AS CEP
    FROM 
        STAGE
''')

## Função para salvar dataframe ##

salvar_df(ft_vendas, 'ft_vendas')

salvar_df(df_dim_clientes, 'dim_clientes')

salvar_df(df_dim_tempo, 'dim_tempo')

salvar_df(df_dim_localidade, 'dim_localidade')

