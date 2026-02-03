#!/bin/bash

# Cores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}Inicializando Hadoop + Spark${NC}"
echo -e "${YELLOW}========================================${NC}"

# 1. Iniciar Docker Compose
echo -e "\n${YELLOW}[1/6] Iniciando containers Docker...${NC}"
docker compose up -d

# 2. Aguardar inicialização
echo -e "${YELLOW}[2/6] Aguardando inicialização (120s)...${NC}"
sleep 120

# 3. Sair do Safe Mode
echo -e "${YELLOW}[3/6] Saindo do Safe Mode do HDFS...${NC}"
docker exec namenode hdfs dfsadmin -safemode leave

# 4. Criar diretórios
echo -e "${YELLOW}[4/6] Criando diretórios no HDFS...${NC}"
docker exec namenode hdfs dfs -mkdir -p /data/vendas /data/compras /user/hive/warehouse
docker exec namenode hdfs dfs -chmod -R 777 /data /user

# 5. Copiar dados
echo -e "${YELLOW}[5/6] Copiando dados para o HDFS...${NC}"
docker cp ./dados/vendas.csv namenode:/tmp/vendas.csv
docker cp ./dados/compras.csv namenode:/tmp/compras.csv

docker exec namenode hdfs dfs -put -f /tmp/vendas.csv /data/vendas/
docker exec namenode hdfs dfs -put -f /tmp/compras.csv /data/compras/

# 6. Verificar
echo -e "${YELLOW}[6/6] Verificando dados...${NC}"
docker exec namenode hdfs dfs -ls /data/vendas/
docker exec namenode hdfs dfs -ls /data/compras/

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}✓ Ambiente inicializado com sucesso!${NC}"
echo -e "${GREEN}========================================${NC}"

echo -e "\n${YELLOW}Interfaces Web:${NC}"
echo -e "  Hadoop NameNode: http://localhost:9870"
echo -e "  ResourceManager: http://localhost:8088"
echo -e "  Spark Master: http://localhost:8080"
echo -e "  Portainer: http://localhost:9000"
echo -e "\n${YELLOW}Próximo passo: execute ./processar_dados.sh${NC}\n"
