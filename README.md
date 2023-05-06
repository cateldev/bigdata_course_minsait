# Curso Big Data Minsait

Este curso foi realizado durante o programa de trainee disponibilizado pela Indra-Minsait

----------------------------------------------------------------------------------------------

Objetivo 

Curso realizado com o intuito de aprender a manipular e tratar dados oriundos do big data para que possam ser utilizados
pelas companhias durante suas análises estratégicas.

-----------------------------------------------------------------------------------------------

Instruções & Etapas do projeto

Iniciando o cluster de docker:

docker-compose up -d

Acessando os containers namenode & hive:

docker exec -it namenode bash</br>
docker exec -it hive-server bash

Criando pastas para armazenar os arquivos csvs:

mkdir input/raw/<folder>

Criando as pastas datalake e raw no HDFS:

hdfs dfs -mkdir /datalake/raw/<folder>/

Movendo os arquivos de input/raw para o datalake via hadoop:

hdfs dfs -copyFromLocal <file>.csv /datalake/raw/<folder>/ 

Verificar se o arquivo está dentro do hdfs:

hdfs dfs –ls /datalake/raw/<folder>/

Acessar o hive com a string de conexão no terminal:

beeline -u jdbc:hive2://localhost:10000

Criando e usando databases dentro do beeline:

create database <database>;
use <database>;

Verificar as tabelas com o camando:

show tables;

---------------------------------------------------------------------

Para manipular os dados via Pyspark:

Liberar a PORT 8889 e acessar o link

Manipulação feita via Jupyter
