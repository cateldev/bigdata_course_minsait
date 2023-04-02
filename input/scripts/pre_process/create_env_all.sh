#!/bin/bash

# Criação das pastas

# Permissão adm: chmod +x create_env_all.sh

# Executando o arquivo: ./create_env_all.sh

DADOS=("clientes" "divisao" "endereco" "regiao" "vendas")

for i in "${DADOS[@]}"
do

    cd ../../raw
    echo "#############################"
	echo "$i"
    echo "Criando datalake de $i"
    hdfs dfs -mkdir /datalake/raw/$i
    echo "#############################"
    echo "datalake de $i criado, agora dando permissões"
    hdfs dfs -chmod 777 /datalake/raw/$i
    echo "#############################"
    echo "Permissões de $i concedidas, copiando arquvios para o datalake"
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
    echo "#############################"
    echo "arquivo $i.csv copiado para datalake"
    echo "#############################"
done

hdfs dfs -ls /datalake/raw/
echo "Operação finalizada"

echo "Iniciando operação de criação de tabelas"
beeline -u jdbc:hive2://localhost:10000
echo "Acessando database minsait"
use database minsait;

for i in "${DADOS[@]}"
do  
    echo "#############################"
    echo "criando tabela de $i"
    beeline -f ../../scripts/hql/create_table_$i.hql;
    echo "#############################"
done

show tables;