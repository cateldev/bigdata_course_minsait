#!/bin/bash

# Criação das pastas

# Permissão adm: chmod +x create_env_all.sh

# Executando o arquivo: ./create_env_all.sh

DADOS=("clientes" "divisao" "endereco" "regiao" "vendas")

for i in "${DADOS[@]}"
do
	echo "$i"
    cd ../../raw/
    hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
    beeline -u jdbc:hive2://localhost:10000 -f ../../scripts/hql/create_table_$i.hql 
done
