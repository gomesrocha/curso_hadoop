# Curso Completo de PySpark 4.0: Do Básico ao Avançado

## 🚀 Visão Geral do Curso

Bem-vindo ao Curso Completo de PySpark 4.0! Este material foi cuidadosamente elaborado para fornecer uma jornada de aprendizado profunda e prática no mundo do processamento de Big Data com Apache Spark.

Diferente de outros materiais, este curso é focado na **versão 4.0 do PySpark**, com exemplos e configurações otimizadas para o **Google Colab**, e utiliza um conjunto de dados de ecommerce real (`clientes`, `produtos`, `vendas`) para criar laboratórios práticos e contextualizados.
 
**Plataforma**: Google Colab

## 🎯 Objetivos de Aprendizagem

Ao final deste curso, você será capaz de:

*   Entender a arquitetura e os fundamentos do Apache Spark.
*   Configurar e otimizar um ambiente PySpark 4.0 no Google Colab.
*   Manipular, filtrar e transformar grandes volumes de dados com a API de DataFrames.
*   Realizar análises complexas, cruzando dados de múltiplas fontes com `join` e `groupBy`.
*   Utilizar o poder do Spark SQL para executar consultas SQL em Big Data.
*   Construir, treinar e avaliar modelos de Machine Learning em escala com a biblioteca MLlib.
*   Processar dados em tempo real com o Structured Streaming.
*   Diagnosticar gargalos e otimizar a performance de suas aplicações Spark usando a Spark UI.


## 🛠️ Como Começar

1.  **Acesse o Google Colab**: Abra [Google Colab](https://colab.research.google.com/drive/1QlQiUuRNcbWFW3xLuY1BoUWlrPyZGxL8?usp=sharing).
2.  **Faça o Upload dos Datasets**: Faça o upload dos arquivos `clientes.csv`, `produtos.csv`, e `vendas.csv` (localizados no github) para o seu ambiente Colab.
3.  **Siga os Módulos**: Prossiga pelos módulos na ordem, lendo a teoria e executando os laboratórios práticos no seu notebook.

## 💡 Diferenciais Deste Curso

*   **Foco no PySpark 4.0**: Utiliza a versão mais recente do Spark, com suas novas funcionalidades e otimizações.
*   **Exemplos Práticos e Reais**: Todas as análises são baseadas em um cenário de ecommerce real, respondendo a perguntas de negócio concretas.
*   **Explicações Detalhadas**: Foco em explicar o **"porquê"** e o **"como"** por trás de cada conceito, como o funcionamento interno de um `join` ou de um `groupBy`.
*   **Mão na Massa**: O curso é centrado em laboratórios práticos que constroem conhecimento de forma incremental.
*   **Otimização desde o Início**: Introduz conceitos de performance e o uso da Spark UI para que você aprenda a escrever código eficiente.

--- 

**Pronto para dominar o processamento de Big Data? Vamos começar!**
# Módulo 0: Configurando o Ambiente com PySpark 4.0 no Google Colab

## 1. Introdução

Bem-vindo ao curso de PySpark! O primeiro passo em nossa jornada é configurar um ambiente de desenvolvimento robusto e eficiente. Para isso, utilizaremos o **Google Colaboratory (Colab)**, uma plataforma gratuita que oferece acesso a recursos computacionais na nuvem, eliminando a necessidade de instalações complexas em sua máquina local.

Neste módulo, vamos instalar e configurar o **PySpark 4.0**, a versão mais recente do Spark, que traz melhorias significativas de performance e usabilidade, especialmente em ambientes como o Colab.

## 2. Por que PySpark 4.0 e Java 17?

- **PySpark 4.0**: Traz otimizações no Catalyst Optimizer, melhorias na API do Pandas e uma integração mais fluida com o ecossistema Python moderno.
- **Java 17 (JDK 17)**: O Spark 4.0 é construído sobre o Scala 2.13, que por sua vez, funciona melhor com versões mais recentes do Java. O uso do JDK 17 garante compatibilidade e acesso às últimas melhorias de performance e segurança da JVM.
- **`pyspark[connect]`**: Instalaremos o PySpark com o extra `[connect]`, que habilita o **Spark Connect**. O Spark Connect introduz uma arquitetura desacoplada cliente-servidor, permitindo que você execute suas aplicações Spark de qualquer lugar (seu notebook, um IDE local) e se conecte a um cluster Spark remoto. Isso melhora a interatividade e a experiência de desenvolvimento.

## 3. Script de Instalação para o Google Colab

Para começar, crie um novo notebook no Google Colab. Na primeira célula, cole e execute o seguinte código. Este script cuidará de toda a instalação e configuração necessárias.

```python
# 1. Atualizar os pacotes do sistema operacional
!apt-get -qq update

# 2. Instalar o Java Development Kit (JDK) 17
# O PySpark 4.0 funciona melhor com versões mais recentes do Java.
!apt-get -qq install -y openjdk-17-jdk-headless

# 3. Instalar o PySpark 4.0 com suporte ao Spark Connect
# O -U garante que estamos instalando ou atualizando para a versão especificada.
!pip -q install -U pyspark[connect]==4.0.0

# 4. Mensagem de sucesso
print('✅ PySpark 4.0 instalado com sucesso!')

# 5. AVISO IMPORTANTE:
# Após a execução desta célula, você DEVE reiniciar o ambiente de execução.
# Vá em "Ambiente de execução" > "Reiniciar ambiente de execução" no menu do Colab.
# Isso é crucial para que as novas bibliotecas e variáveis de ambiente sejam carregadas corretamente.
```

### **Atenção: Reinicie o Ambiente de Execução!**

Após a instalação ser concluída, é **obrigatório** reiniciar o ambiente de execução do Colab. Você pode fazer isso clicando em **"Ambiente de execução"** na barra de menu e depois em **"Reiniciar ambiente de execução"**. Se você não fizer isso, o notebook não encontrará a nova instalação do PySpark e os próximos passos falharão.

## 4. Iniciando a `SparkSession`

Após reiniciar o ambiente, você pode iniciar sua `SparkSession`. A `SparkSession` é o ponto de entrada para qualquer aplicação Spark. Em uma nova célula, execute o seguinte código:

```python
# Importar a SparkSession do PySpark
from pyspark.sql import SparkSession

# Construir e criar a SparkSession
# .master("local[*]") -> Executa o Spark localmente usando todos os cores de CPU disponíveis
# .appName("CursoHadoop") -> Define um nome para sua aplicação
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("CursoHadoop") \
    .getOrCreate()

# Verificar se a sessão foi criada com sucesso
print("SparkSession criada com sucesso!")
print(spark)

# Exibir a versão do Spark
print(f"Versão do Spark: {spark.version}")
```

Se tudo correu bem, você verá uma mensagem de sucesso e os detalhes da sua `SparkSession`, confirmando que você está executando a versão 4.0.0.

## 5. Carregando os Dados do Curso

Para os laboratórios deste curso, utilizaremos três arquivos CSV que você forneceu: `clientes.csv`, `produtos.csv` e `vendas.csv`.

Primeiro, faça o upload desses arquivos para o seu ambiente Colab. Você pode fazer isso clicando no ícone de pasta na barra lateral esquerda e, em seguida, no botão "Fazer upload".

Depois de fazer o upload, você pode carregar os dados em DataFrames do PySpark:

```python
# Carregar os datasets em DataFrames
# header=True -> Usa a primeira linha como cabeçalho
# inferSchema=True -> Tenta inferir os tipos de dados das colunas (pode ser lento para arquivos grandes)

clientes_df = spark.read.csv('clientes.csv', header=True, inferSchema=True)
produtos_df = spark.read.csv('produtos.csv', header=True, inferSchema=True)
vendas_df = spark.read.csv('vendas.csv', header=True, inferSchema=True)

# Exibir as primeiras linhas e o schema de cada DataFrame para verificar
print("--- Clientes ---")
clientes_df.show(5)
clientes_df.printSchema()

print("--- Produtos ---")
produtos_df.show(5)
produtos_df.printSchema()

print("--- Vendas ---")
vendas_df.show(5)
vendas_df.printSchema()
```

Com o ambiente configurado e os dados carregados, você está pronto para começar a explorar o poder do PySpark nos próximos módulos!
# Módulo 1: Fundamentos do Apache Spark

## Capítulo 1: O que é Apache Spark?

### 1.1. Definindo o Apache Spark

O **Apache Spark** é uma plataforma de computação distribuída de código aberto, unificada e de alta velocidade, projetada para o processamento de dados em larga escala (Big Data). Ele se destaca por sua capacidade de executar tarefas de análise de dados de forma significativamente mais rápida do que tecnologias antecessoras, como o Hadoop MapReduce, principalmente por realizar o processamento em memória.

Em sua essência, o Spark é um motor de processamento. Ele não armazena dados de forma nativa e permanente, mas se integra a uma vasta gama de sistemas de armazenamento, como Hadoop Distributed File System (HDFS), Amazon S3, Google Cloud Storage, e bancos de dados NoSQL como Cassandra e HBase. Essa flexibilidade permite que o Spark atue como uma camada de processamento poderosa sobre os dados, onde quer que eles residam.

> "O Spark foi projetado para cobrir uma ampla gama de cargas de trabalho que anteriormente exigiam sistemas distribuídos separados, incluindo processamento em lote, consultas interativas, análise de streaming, aprendizado de máquina e processamento de grafos." - Documentação Oficial do Apache Spark

### 1.2. A Evolução: Do Hadoop MapReduce ao Spark

Para entender a importância do Spark, é útil olhar para seu predecessor, o **Hadoop MapReduce**. O MapReduce foi pioneiro no processamento de Big Data, mas possuía uma limitação fundamental: ele dependia intensivamente de operações de leitura e escrita em disco. Cada etapa de um job MapReduce escrevia seus resultados intermediários no HDFS, o que gerava uma alta latência, especialmente para algoritmos iterativos (como os de machine learning) e análises interativas.

O Spark surgiu para superar essa limitação. Sua principal inovação foi a introdução do conceito de **Resilient Distributed Dataset (RDD)**, uma estrutura de dados que permite que os dados sejam mantidos em memória entre as operações. Ao minimizar o I/O (Input/Output) de disco, o Spark alcançou ganhos de performance de até 100 vezes em comparação com o MapReduce para certas aplicações.

| Característica | Hadoop MapReduce | Apache Spark |
| :--- | :--- | :--- |
| **Processamento** | Baseado em disco | Primariamente em memória |
| **Latência** | Alta | Baixa |
| **Velocidade** | Lento para cargas de trabalho iterativas | Muito rápido para cargas de trabalho iterativas e interativas |
| **API** | API de baixo nível (Map, Reduce) | APIs de alto nível (DataFrames, SQL, Streaming) |
| **Ecossistema** | Requer diferentes ferramentas para diferentes tarefas | Plataforma unificada para múltiplas tarefas |

### 1.3. O Ecossistema Unificado do Spark

Uma das maiores forças do Spark é seu ecossistema coeso, que oferece bibliotecas integradas para diversas necessidades de análise de dados. Isso permite que os desenvolvedores construam aplicações complexas de ponta a ponta usando uma única plataforma.

Os principais componentes do ecossistema Spark são:

*   **Spark Core**: É a base da plataforma. Ele fornece a funcionalidade principal do Spark, incluindo o agendamento de tarefas, gerenciamento de memória, tratamento de falhas e a API de RDDs.

*   **Spark SQL**: Construído sobre o Spark Core, o Spark SQL introduz a abstração de **DataFrame** e permite a consulta de dados estruturados e semi-estruturados usando a sintaxe SQL. É um dos componentes mais utilizados do Spark.

*   **Spark Streaming e Structured Streaming**: Bibliotecas para o processamento de fluxos de dados em tempo real. O **Structured Streaming**, a API mais recente, trata os fluxos de dados como tabelas que são continuamente atualizadas, simplificando enormemente o desenvolvimento de aplicações de streaming.

*   **MLlib (Machine Learning Library)**: A biblioteca de machine learning do Spark, que oferece uma ampla gama de algoritmos de classificação, regressão, clusterização e filtragem colaborativa, além de ferramentas para construção de pipelines de ML em escala.

*   **GraphX**: Uma API para processamento de grafos e computação paralela de grafos, útil para análises de redes sociais, sistemas de recomendação e modelagem de redes.

### 1.4. Vantagens e Casos de Uso

O Apache Spark se tornou a ferramenta padrão para muitos cenários de Big Data devido às suas vantagens distintas:

**Vantagens:**

*   **Velocidade**: Graças ao processamento em memória e ao otimizador de consultas avançado (Catalyst), o Spark é extremamente rápido.
*   **Facilidade de Uso**: Oferece APIs de alto nível e expressivas em Python (PySpark), Scala, Java e R, tornando o desenvolvimento mais produtivo.
*   **Plataforma Unificada**: Combina processamento em lote, streaming, SQL e machine learning em uma única ferramenta, reduzindo a complexidade do sistema.
*   **Flexibilidade**: Pode ser executado em diversos ambientes (Hadoop YARN, Kubernetes, Mesos, Standalone) e se conectar a inúmeras fontes de dados.

**Casos de Uso Comuns:**

*   **Engenharia de Dados e ETL**: Processamento e transformação de grandes volumes de dados (ETL - Extract, Transform, Load) de forma rápida e escalável.
*   **Análise de Dados Interativa**: Analistas podem explorar terabytes de dados de forma interativa usando Spark SQL e notebooks (como o Colab).
*   **Machine Learning em Larga Escala**: Treinamento de modelos de machine learning em datasets que não cabem em uma única máquina.
*   **Processamento de Dados em Tempo Real**: Análise de dados de streaming para detecção de fraudes, monitoramento de sistemas, personalização em tempo real e análise de dados de IoT.

No próximo capítulo, vamos mergulhar na arquitetura do Spark para entender como ele alcança essa performance e escalabilidade e velocidade.
# Módulo 1: Fundamentos do Apache Spark

## Capítulo 2: Arquitetura do Apache Spark

### 2.1. Visão Geral da Arquitetura

Para dominar o Spark, é essencial compreender sua arquitetura. O Spark opera em um modelo de cluster mestre-escravo (master-slave), onde um nó central coordena o trabalho que é distribuído entre vários nós de trabalho. Essa arquitetura é o que permite ao Spark processar dados em paralelo e em grande escala.

Uma aplicação Spark é composta por dois tipos principais de processos:

1.  **Programa Driver (Driver Program)**: O processo mestre.
2.  **Executores (Executors)**: Os processos de trabalho (escravos).

Esses processos são gerenciados por um **Gerenciador de Cluster (Cluster Manager)**, que é responsável por alocar os recursos do cluster para a aplicação.

![Arquitetura do Spark](https://spark.apache.org/docs/latest/img/cluster-overview.png)
*Fonte: Documentação Oficial do Apache Spark*

### 2.2. Os Componentes em Detalhe

#### **Programa Driver (Driver Program)**

O Driver é o coração da sua aplicação Spark. É o processo onde o método `main()` da sua aplicação é executado. Suas principais responsabilidades são:

*   **Criar o `SparkContext`**: O `SparkContext` (ou a `SparkSession`, que o encapsula) é a conexão principal com o cluster Spark.
*   **Analisar, Otimizar e Planejar**: O Driver pega o código do usuário (as transformações e ações) e o converte em um plano de execução físico. Ele cria um **Grafo Acíclico Dirigido (DAG)** de operações.
*   **Agendar Tarefas**: O Driver divide o plano de execução em unidades menores de trabalho chamadas **tarefas (tasks)** e as envia para os executores.
*   **Coordenar a Execução**: Ele monitora o progresso das tarefas e reagenda as que falham.

Em um ambiente como o Google Colab, o próprio notebook Python atua como o Programa Driver.

#### **Executores (Executors)**

Os Executores são os processos de trabalho que rodam nos nós do cluster (worker nodes). Eles são lançados no início de uma aplicação Spark e permanecem ativos durante toda a sua duração. Cada executor tem duas funções principais:

1.  **Executar Tarefas**: Eles recebem as tarefas do Driver e as executam nos dados. Cada tarefa opera em uma partição específica dos dados.
2.  **Armazenar Dados**: Eles armazenam em cache (em memória ou em disco) as partições de dados que foram persistidas pelo usuário através de `cache()` ou `persist()`.

Cada executor possui um número alocado de **cores** (ou slots de tarefas), que determina quantas tarefas ele pode executar simultaneamente. Por exemplo, um executor com 4 cores pode executar 4 tarefas em paralelo.

#### **Gerenciador de Cluster (Cluster Manager)**

O Gerenciador de Cluster é responsável por alocar os recursos físicos (CPU, memória) do cluster para as aplicações Spark. O Spark é agnóstico em relação ao gerenciador de cluster e pode ser executado em vários deles:

*   **Standalone**: Um gerenciador de cluster simples que vem com o próprio Spark. É fácil de configurar e ótimo para começar.
*   **Apache Hadoop YARN**: O gerenciador de recursos do Hadoop. É a escolha mais comum em ambientes de produção que já possuem um ecossistema Hadoop.
*   **Apache Mesos**: Um gerenciador de cluster de propósito geral que pode executar diversas aplicações, incluindo Spark.
*   **Kubernetes**: Um sistema de orquestração de contêineres que se tornou uma opção popular para executar o Spark, especialmente em ambientes de nuvem.

Quando você executa o Spark no modo `local[*]`, como fazemos no Colab, você está usando um modo especial onde o Driver e um Executor rodam dentro do mesmo processo na mesma máquina, simulando um ambiente de cluster.

### 2.3. O Fluxo de Execução de uma Aplicação Spark

Vamos visualizar o ciclo de vida de uma aplicação Spark:

1.  **Submissão**: Você submete sua aplicação (por exemplo, um script Python) ao Spark.
2.  **Início do Driver**: O Gerenciador de Cluster aloca recursos para o Programa Driver e o inicia.
3.  **Criação do `SparkContext`**: O Driver cria o `SparkContext`, que se conecta ao Gerenciador de Cluster.
4.  **Alocação dos Executores**: O `SparkContext` solicita recursos ao Gerenciador de Cluster para os Executores.
5.  **Lançamento dos Executores**: O Gerenciador de Cluster lança os processos Executores nos nós de trabalho.
6.  **Execução do Código**: O Driver executa o código do usuário. Quando uma **ação** é encontrada, o Driver cria o DAG, otimiza-o e o divide em estágios e tarefas.
7.  **Agendamento de Tarefas**: O Driver envia as tarefas para os Executores.
8.  **Execução das Tarefas**: Os Executores executam as tarefas em suas partições de dados e retornam os resultados para o Driver (se necessário).
9.  **Fim da Aplicação**: Quando a aplicação termina, o `SparkContext` é parado e o Gerenciador de Cluster libera todos os recursos.

### 2.4. O Grafo Acíclico Dirigido (DAG)

O DAG é um conceito central para a performance do Spark. Em vez de executar as operações uma a uma, o Spark constrói um grafo de todas as transformações que você define. Este grafo é "acíclico" porque as operações fluem em uma única direção, e "dirigido" porque cada operação depende da anterior.

Quando uma ação é chamada, o **DAG Scheduler** entra em ação. Ele olha para o grafo e o divide em **estágios (stages)**. Um estágio é um conjunto de transformações que podem ser executadas juntas, sem a necessidade de um **shuffle** (a custosa operação de redistribuir dados pela rede). Uma nova etapa é criada sempre que os dados precisam ser embaralhados.

Essa abordagem permite que o Spark otimize a execução, combinando operações e minimizando a movimentação de dados, o que é fundamental para seu desempenho.

Compreender essa arquitetura é o primeiro passo para escrever aplicações PySpark eficientes e para ser capaz de diagnosticar e otimizar problemas de performance, um tópico que exploraremos em detalhes em um módulo posterior.
# Módulo 2: Operações com DataFrames

## Laboratório 1: Manipulação Básica de DataFrames

### 1. Objetivo

Neste primeiro laboratório, vamos nos familiarizar com as operações mais fundamentais e essenciais para trabalhar com DataFrames no PySpark. Usaremos os datasets que você forneceu (`clientes.csv`, `produtos.csv`, `vendas.csv`) para realizar tarefas de exploração, seleção, filtragem e manipulação de dados. O objetivo é construir uma base sólida para as análises mais complexas que faremos nos próximos módulos.

### 2. Pré-requisitos

*   Ambiente PySpark 4.0 configurado no Google Colab (conforme o Módulo 0).
*   `SparkSession` iniciada.
*   Arquivos `clientes.csv`, `produtos.csv`, e `vendas.csv` carregados no ambiente do Colab.

### 3. Carregando e Inspecionando os Dados

Vamos começar carregando nossos arquivos CSV em DataFrames e realizando uma inspeção inicial para entender sua estrutura.

```python
# Certifique-se de que sua SparkSession está ativa
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("CursoHadoop_Lab1").getOrCreate()

# Carregar os datasets
clientes_df = spark.read.csv('clientes.csv', header=True, inferSchema=True)
produtos_df = spark.read.csv('produtos.csv', header=True, inferSchema=True)
vendas_df = spark.read.csv('vendas.csv', header=True, inferSchema=True)
```

#### **Inspeção Básica**

As primeiras ações que realizamos em um novo DataFrame são para entender sua forma e conteúdo.

*   **`printSchema()`**: Mostra a "planta" do seu DataFrame, exibindo os nomes das colunas, seus tipos de dados e se permitem valores nulos.
*   **`show(n)`**: Exibe as primeiras `n` linhas do DataFrame em um formato de tabela. É a maneira mais rápida de ter uma noção dos dados.
*   **`count()`**: Retorna o número total de linhas no DataFrame. Esta é uma **ação**, o que significa que o Spark executará todo o processamento necessário para calcular o resultado.

```python
# Inspecionando o DataFrame de Vendas
print("Schema do DataFrame de Vendas:")
vendas_df.printSchema()

print("\nExibindo as 5 primeiras vendas:")
vendas_df.show(5)

print(f"\nNúmero total de registros de vendas: {vendas_df.count()}")

# Inspecionando o DataFrame de Clientes
print("\nSchema do DataFrame de Clientes:")
clientes_df.printSchema()

print(f"\nNúmero total de clientes: {clientes_df.count()}")
```

### 4. Selecionando Colunas (`select`)

A operação `select` é usada para escolher uma ou mais colunas de um DataFrame, criando um novo DataFrame com apenas as colunas selecionadas. É análoga à cláusula `SELECT` do SQL.

```python
# Selecionar colunas específicas do DataFrame de vendas
vendas_info_basica_df = vendas_df.select("venda_id", "cliente_id", "data_venda", "preco_total")

print("DataFrame de vendas com colunas selecionadas:")
vendas_info_basica_df.show(5)

# Você também pode passar os nomes das colunas como strings separadas
produtos_info_df = produtos_df.select("nome_produto", "categoria", "preco")
produtos_info_df.show(5)
```

### 5. Filtrando Dados (`filter` e `where`)

Filtrar é uma das operações mais comuns. Você usa `filter()` ou `where()` (são sinônimos) para selecionar linhas que atendem a uma determinada condição. As condições são expressas usando a notação de colunas.

```python
from pyspark.sql.functions import col

# Filtrar vendas com preço total acima de 5000
vendas_caras_df = vendas_df.filter(col("preco_total") > 5000)

print(f"Número de vendas com preço total > 5000: {vendas_caras_df.count()}")
vendas_caras_df.show(5)

# Filtrar produtos da categoria "Eletrônicos"
# Usando a sintaxe de string para a condição
produtos_eletronicos_df = produtos_df.filter("categoria = 'Eletrônicos'")

print("\nProdutos da categoria Eletrônicos:")
produtos_eletronicos_df.show(5)

# Combinando múltiplas condições
# Queremos clientes do estado de São Paulo (SP) ou Rio de Janeiro (RJ)
clientes_sp_rj_df = clientes_df.filter(
    (col("estado") == "SP") | (col("estado") == "RJ")
)

print(f"\nNúmero de clientes de SP ou RJ: {clientes_sp_rj_df.count()}")
clientes_sp_rj_df.show(5)
```

### 6. Criando e Renomeando Colunas (`withColumn` e `withColumnRenamed`)

*   **`withColumn(nome_nova_coluna, expressao)`**: Adiciona uma nova coluna a um DataFrame ou substitui uma existente. A `expressao` define como os valores da nova coluna são calculados.
*   **`withColumnRenamed(nome_antigo, nome_novo)`**: Simplesmente renomeia uma coluna existente.

Vamos usar `withColumn` para calcular o preço médio por item em cada venda.

```python
# Calcular o preço médio por item na venda
vendas_com_preco_medio_df = vendas_df.withColumn(
    "preco_medio_item", 
    col("preco_total") / col("quantidade")
)

print("Vendas com a nova coluna 'preco_medio_item':")
vendas_com_preco_medio_df.select("venda_id", "quantidade", "preco_total", "preco_medio_item").show(5)

# Renomear a coluna 'preco' em produtos_df para 'preco_unitario'
produtos_renomeado_df = produtos_df.withColumnRenamed("preco", "preco_unitario")

print("\nDataFrame de produtos com coluna renomeada:")
produtos_renomeado_df.printSchema()
```

### 7. Ordenando os Dados (`orderBy`)

A operação `orderBy` (ou `sort`) é usada para ordenar as linhas de um DataFrame com base em uma ou mais colunas.

```python
from pyspark.sql.functions import desc, asc

# Encontrar as 10 vendas mais caras
print("Top 10 vendas mais caras:")
vendas_df.orderBy(desc("preco_total")).show(10)

# Encontrar os 10 produtos mais baratos
print("\nTop 10 produtos mais baratos:")
produtos_df.orderBy(col("preco").asc()).show(10) # .asc() é o padrão, mas é bom ser explícito
```

### 8. Removendo Colunas (`drop`)

Se você precisar remover colunas de um DataFrame, pode usar o método `drop()`.

```python
# No DataFrame de clientes, a coluna 'email' pode não ser necessária para uma análise agregada
clientes_sem_email_df = clientes_df.drop("email")

print("DataFrame de clientes sem a coluna 'email':")
clientes_sem_email_df.printSchema()
```

### 9. Conclusão do Laboratório

Parabéns! Você completou o primeiro laboratório prático. Agora você está familiarizado com as operações mais essenciais para inspecionar, selecionar, filtrar e transformar DataFrames no PySpark. Essas são as ferramentas que você usará repetidamente em qualquer análise de dados.

No próximo laboratório, vamos explorar operações mais avançadas, como agregações (`groupBy`) e junções (`join`), para começar a cruzar informações entre nossos diferentes datasets.
# Módulo 2: Operações com DataFrames

## Laboratório 2: Agregações e Junções (`groupBy` e `join`)

### 1. Objetivo

No laboratório anterior, aprendemos a manipular um único DataFrame. Agora, vamos dar um passo adiante e explorar duas das operações mais poderosas e fundamentais em qualquer sistema de análise de dados: **agregações** e **junções**. O verdadeiro poder do Spark (e do SQL em geral) é revelado quando começamos a cruzar informações de diferentes fontes de dados para gerar insights de negócio.

Neste laboratório, vamos responder a perguntas de negócio importantes usando os datasets de clientes, produtos e vendas:

*   Qual o valor total de vendas por categoria de produto?
*   Quais são os produtos mais vendidos (em valor e em quantidade)?
*   Quais são os clientes que mais compram em nossa loja?

Para isso, vamos mergulhar nos conceitos de `groupBy()` e `join()`.

### 2. Agregações com `groupBy()`

A operação `groupBy()` é usada para agrupar linhas que têm os mesmos valores em uma ou mais colunas. Depois de agrupar os dados, você pode aplicar uma ou mais **funções de agregação** para calcular uma métrica resumida para cada grupo.

**Como funciona?**

1.  **Fase de Shuffle (Embaralhamento)**: O Spark redistribui os dados pela rede. Ele garante que todas as linhas com a mesma chave de agrupamento (ex: a mesma `categoria` de produto) terminem na mesma partição, no mesmo nó executor.
2.  **Fase de Agregação**: Em cada partição, o Spark aplica a função de agregação (ex: `sum()`, `count()`, `avg()`) a todas as linhas dentro de cada grupo.

Principais Funções de Agregação:

*   `sum()`: Calcula a soma de uma coluna numérica.
*   `count()`: Conta o número de linhas em um grupo.
*   `avg()`: Calcula a média de uma coluna numérica.
*   `min()`: Encontra o valor mínimo em um grupo.
*   `max()`: Encontra o valor máximo em um grupo.

#### **Exemplo Prático: Total de Vendas e Quantidade por Cliente**

Vamos usar apenas o DataFrame de `vendas_df` para descobrir o valor total e a quantidade total de itens comprados por cada cliente.

```python
from pyspark.sql.functions import sum, count, avg, desc

# Carregar o DataFrame de vendas se ainda não o fez
vendas_df = spark.read.csv('vendas.csv', header=True, inferSchema=True)

# Agrupar por 'cliente_id' e aplicar as agregações
analise_cliente_df = vendas_df.groupBy("cliente_id") \
    .agg(
        sum("preco_total").alias("valor_total_gasto"),
        sum("quantidade").alias("quantidade_total_itens"),
        count("venda_id").alias("numero_de_compras")
    )

print("Análise de gastos por cliente:")
# Ordenar pelos clientes que mais gastaram
analise_cliente_df.orderBy(desc("valor_total_gasto")).show(10)
```

Neste código, `.agg()` é o método que nos permite aplicar múltiplas funções de agregação de uma só vez. Usamos `.alias()` para dar nomes mais descritivos às novas colunas agregadas.

### 3. Junções com `join()`

A operação `join()` é usada para combinar colunas de dois DataFrames com base em uma condição de junção. É a maneira como cruzamos informações, por exemplo, para descobrir o nome de um cliente a partir de seu `cliente_id` em uma venda.

**Como funciona?**

O `join` também envolve uma fase de **shuffle**, onde o Spark redistribui os dados de ambos os DataFrames para que as linhas com a mesma chave de junção (ex: o mesmo `cliente_id`) fiquem no mesmo executor. Uma vez que os dados estão co-localizados, o Spark pode realizar a combinação.

#### **Tipos de Join**

O PySpark suporta todos os tipos de join padrão do SQL. O tipo de join determina como lidar com as chaves que existem em um DataFrame, mas não no outro.

| Tipo de Join | Descrição | Sintaxe PySpark |
| :--- | :--- | :--- |
| **`inner`** | (Padrão) Retorna apenas as linhas onde a chave de junção existe em **ambos** os DataFrames. | `how='inner'` |
| **`left`** | Retorna **todas** as linhas do DataFrame da esquerda e as linhas correspondentes do da direita. Se não houver correspondência, as colunas da direita ficam nulas. | `how='left'` |
| **`right`** | O oposto do `left`. Retorna **todas** as linhas do DataFrame da direita. | `how='right'` |
| **`full_outer`** | Retorna **todas** as linhas quando há uma correspondência em um dos DataFrames. Se não houver correspondência, as colunas do outro DataFrame ficam nulas. | `how='full_outer'` |

### 4. Laboratório Prático: Cruzando Dados de Vendas, Clientes e Produtos

Agora vamos combinar tudo para responder às nossas perguntas de negócio.

#### **Análise 1: Clientes que Mais Compram**

Vamos juntar nossa análise de clientes com o DataFrame de clientes para ver os nomes dos top compradores.

```python
# Carregar os DataFrames
clientes_df = spark.read.csv('clientes.csv', header=True, inferSchema=True)
vendas_df = spark.read.csv('vendas.csv', header=True, inferSchema=True)

# 1. Agrupar as vendas por cliente
analise_cliente_df = vendas_df.groupBy("cliente_id") \
    .agg(
        sum("preco_total").alias("valor_total_gasto"),
        count("venda_id").alias("numero_de_compras")
    )

# 2. Juntar o resultado com o DataFrame de clientes
# A condição de join é a coluna 'cliente_id'
top_clientes_df = analise_cliente_df.join(
    clientes_df, 
    analise_cliente_df.cliente_id == clientes_df.cliente_id, 
    'inner'
)

print("--- Top 10 Clientes por Valor Gasto ---")
top_clientes_df \
    .select("nome", "cidade", "estado", "valor_total_gasto", "numero_de_compras") \
    .orderBy(desc("valor_total_gasto")) \
    .show(10)
```

#### **Análise 2: Vendas por Categoria e Produto**

Para esta análise, precisamos juntar os DataFrames de vendas e produtos.

```python
# Carregar os DataFrames
produtos_df = spark.read.csv('produtos.csv', header=True, inferSchema=True)
vendas_df = spark.read.csv('vendas.csv', header=True, inferSchema=True)

# 1. Juntar vendas com produtos usando 'produto_id'
vendas_produtos_df = vendas_df.join(produtos_df, "produto_id", "inner")

print("DataFrame combinado de Vendas e Produtos:")
vendas_produtos_df.show(5)

# 2. Análise: Vendas totais por Categoria

vendas_por_categoria_df = vendas_produtos_df.groupBy("categoria") \
    .agg(
        sum("preco_total").alias("faturamento_total"),
        sum("quantidade").alias("itens_vendidos")
    )

print("\n--- Faturamento Total por Categoria ---")
vendas_por_categoria_df.orderBy(desc("faturamento_total")).show()

# 3. Análise: Produtos mais vendidos

vendas_por_produto_df = vendas_produtos_df.groupBy("nome_produto", "categoria") \
    .agg(
        sum("preco_total").alias("faturamento_total"),
        sum("quantidade").alias("itens_vendidos")
    )

print("\n--- Top 10 Produtos Mais Vendidos (por faturamento) ---")
vendas_por_produto_df.orderBy(desc("faturamento_total")).show(10)

print("\n--- Top 10 Produtos Mais Vendidos (por quantidade) ---")
vendas_por_produto_df.orderBy(desc("itens_vendidos")).show(10)
```

### 5. Conclusão do Laboratório

Neste laboratório, você aprendeu a usar `groupBy` para agregar dados e `join` para combinar diferentes fontes de informação. Essas duas operações são a espinha dorsal da maioria das análises de dados.

Você foi capaz de transformar dados brutos de vendas, clientes e produtos em insights acionáveis, como identificar seus clientes e produtos mais valiosos. Dominar `groupBy` e `join` é um passo crucial para se tornar proficiente em PySpark.

No próximo módulo, vamos explorar como podemos expressar essas mesmas lógicas usando a sintaxe familiar do **Spark SQL**.
# Módulo 3: Análise de Dados com Spark SQL

## Capítulo 1: Introdução ao Spark SQL

### 1.1. O que é Spark SQL?

**Spark SQL** é um dos módulos mais poderosos e amplamente utilizados do ecossistema Apache Spark. Ele foi projetado para o processamento de dados estruturados e semi-estruturados, trazendo duas contribuições fundamentais para o Spark:

1.  **A API de DataFrames e Datasets**: Uma interface de alto nível para trabalhar com dados de forma tabular, que é mais otimizada e, para muitos, mais intuitiva do que a API de RDDs.
2.  **Um motor SQL completo**: Permite que você execute consultas SQL padrão diretamente sobre seus DataFrames ou sobre fontes de dados externas.

Essencialmente, o Spark SQL preenche a lacuna entre o mundo do SQL tradicional e a análise de dados em larga escala, permitindo que analistas de dados, engenheiros e cientistas de dados usem suas habilidades existentes em SQL para explorar e processar Big Data.

> O Spark SQL permite que os usuários do Spark aproveitem o poder do processamento distribuído e em memória do Spark usando uma sintaxe SQL familiar. Ele se integra perfeitamente com a API de DataFrames, permitindo que você misture consultas SQL com manipulações programáticas de DataFrames na mesma aplicação.

### 1.2. A Mágica por Trás do Spark SQL: Catalyst Optimizer

O coração do Spark SQL e da API de DataFrames é o **Catalyst Optimizer**. O Catalyst é um otimizador de consultas extensível, baseado em árvores, que realiza uma série de otimizações lógicas e físicas no seu código antes de executá-lo.

Quando você escreve uma consulta SQL ou uma operação de DataFrame, o Catalyst a converte em uma árvore de plano lógico. Em seguida, ele aplica um conjunto de regras para otimizar esse plano, como:

*   **Predicate Pushdown**: Empurra as operações de filtro (`WHERE`) para o mais perto possível da fonte de dados. Isso reduz drasticamente a quantidade de dados que precisam ser lidos e processados nas etapas subsequentes.
*   **Column Pruning**: Remove colunas que não são necessárias para a consulta final, minimizando a quantidade de dados lidos e transferidos pela rede.
*   **Join Reordering**: Reordena as junções para garantir que as menores tabelas sejam processadas primeiro, reduzindo a quantidade de dados embaralhados (shuffled).

Após a otimização lógica, o Catalyst gera múltiplos planos físicos e escolhe o mais eficiente com base em um modelo de custos. O resultado final é um código RDD altamente otimizado que é executado no cluster.

É graças ao Catalyst que o código DataFrame/SQL é frequentemente mais rápido do que o código RDD escrito manualmente, pois ele aplica otimizações complexas automaticamente.

### 1.3. Executando Consultas SQL no PySpark

Existem duas maneiras principais de executar consultas SQL no PySpark:

1.  **`spark.sql()`**: Este método, disponível no objeto `SparkSession`, permite executar qualquer consulta SQL como uma string. Ele retorna o resultado como um novo DataFrame.
2.  **Tabelas Temporárias (Temporary Views)**: Para que o Spark SQL possa "ver" um DataFrame, você precisa registrá-lo como uma tabela temporária. Uma "view" não armazena os dados; é apenas um ponteiro para o DataFrame original. Uma vez registrada, você pode referenciar essa view em suas consultas SQL.

#### **Criando uma Tabela Temporária**

Você pode criar uma view temporária usando o método `createOrReplaceTempView()` em um DataFrame.

```python
# Supondo que 'vendas_df' é um DataFrame existente
vendas_df.createOrReplaceTempView("vendas_view")

# Agora você pode usar "vendas_view" em suas consultas SQL
resultado_df = spark.sql("SELECT * FROM vendas_view WHERE quantidade > 5")
```

**Diferença entre `createOrReplaceTempView` e `createGlobalTempView`**

| Método | Escopo | Vida Útil |
| :--- | :--- | :--- |
| `createOrReplaceTempView` | Associado à `SparkSession` atual. | A view é descartada quando a `SparkSession` termina. |
| `createGlobalTempView` | Acessível por todas as `SparkSession`s no mesmo cluster. | A view persiste até que a aplicação Spark termine. Deve ser acessada com o prefixo `global_temp`. |

Para a maioria dos casos de uso, especialmente em notebooks, `createOrReplaceTempView` é suficiente e mais simples de gerenciar.

### 1.4. Vantagens de Usar Spark SQL

*   **Performance**: Graças ao Catalyst Optimizer, as consultas SQL são convertidas em planos de execução altamente eficientes.
*   **Familiaridade e Produtividade**: Permite que qualquer pessoa com conhecimento de SQL comece a trabalhar com Big Data imediatamente, sem precisar aprender uma nova API programática complexa.
*   **Interoperabilidade**: Você pode misturar e combinar perfeitamente a API de DataFrames com consultas SQL na mesma aplicação. O resultado de uma consulta SQL é um DataFrame, que pode ser manipulado posteriormente com a API programática, e vice-versa.
*   **Conectividade**: O Spark SQL pode se conectar a uma ampla variedade de fontes de dados (JDBC, Parquet, JSON, etc.) e expor esses dados para consulta via SQL.

No próximo laboratório, vamos colocar o Spark SQL em prática. Vamos recriar as análises do laboratório anterior (vendas por categoria, top clientes) usando apenas consultas SQL, para que você possa ver o poder e a simplicidade do Spark SQL em ação.
# Módulo 3: Análise de Dados com Spark SQL

## Laboratório: Análise de Vendas com Spark SQL

### 1. Objetivo

Neste laboratório, vamos revisitar as análises que fizemos no módulo anterior, mas desta vez, usaremos exclusivamente o poder do **Spark SQL**. O objetivo é demonstrar como você pode realizar as mesmas operações de agregação e junção usando a sintaxe SQL que você já conhece, diretamente no PySpark.

Ao final deste laboratório, você será capaz de:

*   Registrar DataFrames como tabelas temporárias.
*   Executar consultas `SELECT`, `GROUP BY`, e `JOIN` usando `spark.sql()`.
*   Resolver problemas de negócio complexos combinando a simplicidade do SQL com o poder de processamento do Spark.

### 2. Preparando o Ambiente

Primeiro, vamos carregar nossos DataFrames e registrá-los como views temporárias para que o Spark SQL possa acessá-los.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Iniciar a SparkSession
spark = SparkSession.builder.master("local[*]").appName("CursoHadoop_LabSQL").getOrCreate()

# Carregar os datasets
clientes_df = spark.read.csv("clientes.csv", header=True, inferSchema=True)
produtos_df = spark.read.csv("produtos.csv", header=True, inferSchema=True)
vendas_df = spark.read.csv("vendas.csv", header=True, inferSchema=True)

# Criar views temporárias para cada DataFrame
clientes_df.createOrReplaceTempView("clientes")
produtos_df.createOrReplaceTempView("produtos")
vendas_df.createOrReplaceTempView("vendas")

print("DataFrames carregados e views temporárias criadas com sucesso!")
```

### 3. Análise 1: Clientes que Mais Compram (SQL)

No laboratório anterior, usamos `groupBy` e `join` na API de DataFrames para encontrar os clientes que mais gastaram. Agora, vamos fazer o mesmo com uma única consulta SQL.

**Lógica da Consulta:**

1.  **`FROM vendas`**: Começamos com a tabela de vendas.
2.  **`JOIN clientes ON vendas.cliente_id = clientes.cliente_id`**: Juntamos as vendas com os clientes para obter os nomes e detalhes dos clientes.
3.  **`GROUP BY clientes.cliente_id, clientes.nome, clientes.cidade, clientes.estado`**: Agrupamos por cliente para calcular as métricas de cada um.
4.  **`SELECT ... SUM(preco_total) AS valor_total_gasto, COUNT(venda_id) AS numero_de_compras`**: Calculamos o valor total gasto e o número de compras para cada cliente.
5.  **`ORDER BY valor_total_gasto DESC`**: Ordenamos o resultado para ver os melhores clientes no topo.

```python
query_top_clientes = """
    SELECT 
        c.nome,
        c.cidade,
        c.estado,
        SUM(v.preco_total) AS valor_total_gasto,
        COUNT(v.venda_id) AS numero_de_compras
    FROM vendas v
    JOIN clientes c ON v.cliente_id = c.cliente_id
    GROUP BY c.cliente_id, c.nome, c.cidade, c.estado
    ORDER BY valor_total_gasto DESC
"""

top_clientes_sql_df = spark.sql(query_top_clientes)

print("--- Top 10 Clientes por Valor Gasto (via SQL) ---")
top_clientes_sql_df.show(10)
```

Como você pode ver, o resultado é idêntico ao que obtivemos com a API de DataFrames, mas a lógica está contida em uma única consulta SQL, que pode ser mais legível para quem tem experiência com bancos de dados.

### 4. Análise 2: Vendas por Categoria e Produto (SQL)

Agora, vamos analisar o desempenho dos produtos e categorias, novamente usando SQL.

**Lógica da Consulta (Vendas por Categoria):**

1.  **`FROM vendas`**: Começamos com as vendas.
2.  **`JOIN produtos ON vendas.produto_id = produtos.produto_id`**: Juntamos com os produtos para obter a categoria de cada item vendido.
3.  **`GROUP BY produtos.categoria`**: Agrupamos por categoria.
4.  **`SELECT ... SUM(preco_total) AS faturamento_total, SUM(quantidade) AS itens_vendidos`**: Calculamos o faturamento e o total de itens vendidos por categoria.
5.  **`ORDER BY faturamento_total DESC`**: Ordenamos para ver as categorias mais lucrativas.

```python
# Consulta para faturamento por categoria
query_vendas_categoria = """
    SELECT
        p.categoria,
        SUM(v.preco_total) AS faturamento_total,
        SUM(v.quantidade) AS itens_vendidos
    FROM vendas v
    JOIN produtos p ON v.produto_id = p.produto_id
    GROUP BY p.categoria
    ORDER BY faturamento_total DESC
"""

vendas_categoria_sql_df = spark.sql(query_vendas_categoria)

print("\n--- Faturamento Total por Categoria (via SQL) ---")
vendas_categoria_sql_df.show()

# Consulta para produtos mais vendidos
query_top_produtos = """
    SELECT
        p.nome_produto,
        p.categoria,
        SUM(v.preco_total) AS faturamento_total,
        SUM(v.quantidade) AS itens_vendidos
    FROM vendas v
    JOIN produtos p ON v.produto_id = p.produto_id
    GROUP BY p.nome_produto, p.categoria
    ORDER BY faturamento_total DESC
"""

top_produtos_sql_df = spark.sql(query_top_produtos)

print("\n--- Top 10 Produtos Mais Vendidos (por faturamento, via SQL) ---")
top_produtos_sql_df.show(10)
```

### 5. Misturando SQL e a API de DataFrames

A verdadeira flexibilidade do PySpark vem da capacidade de misturar as duas abordagens. O resultado de `spark.sql()` é um DataFrame, então você pode aplicar mais transformações nele usando a API programática.

Por exemplo, vamos pegar o resultado da nossa consulta de top produtos e adicionar uma coluna que calcula o faturamento médio por item vendido.

```python
# O resultado de spark.sql() é um DataFrame
top_produtos_sql_df = spark.sql(query_top_produtos)

# Agora, usamos a API de DataFrames para adicionar uma nova coluna
produtos_com_media_df = top_produtos_sql_df.withColumn(
    "faturamento_medio_por_item",
    col("faturamento_total") / col("itens_vendidos")
)

print("\n--- Top Produtos com Faturamento Médio por Item ---")
produtos_com_media_df.show(10)
```

### 6. Conclusão

Neste laboratório, você viu como o Spark SQL oferece uma alternativa poderosa e expressiva à API de DataFrames para realizar análises complexas. Para muitas pessoas, escrever uma consulta SQL é mais rápido e mais claro do que encadear múltiplas chamadas de métodos de DataFrame.

Você aprendeu a:

*   Registrar DataFrames como `TempViews`.
*   Executar consultas `SELECT`, `JOIN`, `GROUP BY`, e `ORDER BY`.
*   Combinar a simplicidade do SQL com a flexibilidade da API de DataFrames.

Com o domínio do Spark SQL e da API de DataFrames, você agora tem um conjunto de ferramentas completo para enfrentar praticamente qualquer desafio de análise de dados estruturados no PySpark.

No próximo módulo, vamos mudar nosso foco da análise de dados para o **Machine Learning**, explorando como usar a biblioteca MLlib do Spark para construir modelos preditivos em larga escala.
# Módulo 4: Machine Learning com Spark MLlib

## Capítulo 1: Introdução ao Spark MLlib

### 1.1. O que é MLlib?

**MLlib** é a biblioteca de Machine Learning (Aprendizado de Máquina) do Apache Spark. Seu principal objetivo é tornar o machine learning prático, escalável e fácil de usar. Ela foi projetada para ser executada em paralelo em clusters, permitindo o treinamento de modelos em datasets massivos que não caberiam na memória de uma única máquina.

Desde o Spark 2.0, a API principal da MLlib é baseada em **DataFrames**. Essa API, conhecida como `pyspark.ml`, oferece uma interface de alto nível e uniforme para a criação de modelos e pipelines de machine learning, que se integra perfeitamente com o resto do ecossistema Spark.

> A MLlib contém implementações de alta qualidade e escaláveis de algoritmos comuns de classificação, regressão, clusterização e filtragem colaborativa, além de utilitários para engenharia de features, construção de pipelines e persistência de modelos.

### 1.2. O Paradigma da API `pyspark.ml`: Transformers, Estimators e Pipelines

A API de MLlib baseada em DataFrames é construída em torno de três conceitos principais, que fornecem uma linguagem comum para a criação de fluxos de trabalho de machine learning:

#### **Transformer (Transformador)**

Um **Transformer** é um algoritmo que pode transformar um DataFrame em outro. Geralmente, um Transformer adiciona uma ou mais colunas ao DataFrame de entrada. A lógica da transformação é implementada no método `transform()`.

*   **Exemplo**: Um modelo de machine learning treinado é um Transformer. Ele recebe um DataFrame com as *features* (características) e o transforma em um novo DataFrame que inclui as *previsões*.
*   **Exemplo 2**: Um algoritmo de engenharia de features, como um que converte uma coluna de texto em um vetor numérico, também é um Transformer.

#### **Estimator (Estimador)**

Um **Estimator** é um algoritmo que pode ser treinado (ou "ajustado") em um DataFrame para produzir um Transformer. A lógica de treinamento é implementada no método `fit()`.

*   **Exemplo**: Um algoritmo de classificação, como `LogisticRegression`, é um Estimator. Você chama o método `fit()` em um DataFrame de treinamento (que contém as features e os rótulos verdadeiros), e ele retorna um `LogisticRegressionModel`, que é um Transformer. Este modelo pode então ser usado para fazer previsões em novos dados.

| Conceito | O que faz? | Método Principal | Exemplo |
| :--- | :--- | :--- | :--- |
| **Estimator** | Aprende a partir dos dados. | `fit()` | `LogisticRegression` (o algoritmo) |
| **Transformer** | Transforma os dados. | `transform()` | `LogisticRegressionModel` (o modelo treinado) |

#### **Pipeline (Fluxo de Trabalho)**

Um **Pipeline** encadeia múltiplos Transformers e Estimators para criar um fluxo de trabalho de machine learning completo e unificado. Um Pipeline é, ele próprio, um Estimator.

Quando você chama `fit()` em um Pipeline, ele executa cada estágio em ordem. Se o estágio for um Estimator, ele chama `fit()` para treiná-lo e obter um Transformer. Se o estágio já for um Transformer, ele simplesmente o passa para a próxima etapa. O resultado de `fit()` em um Pipeline é um `PipelineModel`, que é um Transformer.

Os Pipelines são extremamente úteis porque automatizam todo o fluxo de trabalho, desde o pré-processamento dos dados e engenharia de features até o treinamento do modelo. Isso garante que as mesmas transformações sejam aplicadas de forma consistente tanto nos dados de treinamento quanto nos de teste.

### 1.3. Etapas Comuns em um Fluxo de Trabalho de MLlib

Um projeto típico de machine learning com PySpark MLlib segue estas etapas:

1.  **Preparação dos Dados**: Carregar os dados como um DataFrame e realizar a limpeza inicial.
2.  **Engenharia de Features (Feature Engineering)**: Usar uma série de **Transformers** para converter os dados brutos em *features* numéricas que o modelo possa entender. Isso inclui:
    *   `StringIndexer`: Converter colunas de string (categóricas) em índices numéricos.
    *   `OneHotEncoder`: Converter os índices numéricos em vetores one-hot.
    *   `VectorAssembler`: Agrupar múltiplas colunas de features em uma única coluna de vetor. **Esta é uma etapa obrigatória**, pois os algoritmos da MLlib esperam uma única coluna de features do tipo vetor.
3.  **Divisão dos Dados**: Dividir o DataFrame em conjuntos de treinamento e teste usando `randomSplit()`.
4.  **Definição do Modelo**: Instanciar o **Estimator** do modelo que você deseja treinar (ex: `LinearRegression`, `DecisionTreeClassifier`).
5.  **Criação do Pipeline**: Montar um **Pipeline** com todos os estágios de engenharia de features e o Estimator do modelo.
6.  **Treinamento do Modelo**: Chamar `fit()` no Pipeline com o conjunto de treinamento. Isso executará todo o fluxo e retornará um `PipelineModel` treinado.
7.  **Realização de Previsões**: Chamar `transform()` no `PipelineModel` com o conjunto de teste para gerar as previsões.
8.  **Avaliação do Modelo**: Usar um dos **Evaluators** da MLlib (ex: `RegressionEvaluator`, `BinaryClassificationEvaluator`) para medir a performance do modelo nas previsões geradas.

### 1.4. Vantagens da Abordagem de Pipeline

*   **Consistência**: Garante que o pré-processamento seja aplicado da mesma forma nos dados de treino, teste e produção, evitando erros comuns.
*   **Simplicidade**: Simplifica o gerenciamento de fluxos de trabalho complexos com muitas etapas.
*   **Persistência**: Você pode salvar e carregar todo o Pipeline treinado (`PipelineModel`) em disco, o que facilita a implantação do modelo em produção.

No próximo laboratório, vamos aplicar todos esses conceitos para construir nosso primeiro modelo de machine learning com PySpark, usando um dataset real da internet para prever um resultado de forma de pagamento.
# Módulo 4: Machine Learning com Spark MLlib

## Laboratório: Construindo um Modelo de Classificação para Prever Formas de Pagamento

### 1. Objetivo

Neste laboratório, vamos construir nosso primeiro modelo de machine learning de ponta a ponta usando o PySpark. O objetivo é prever qual a **forma de pagamento** (`payment_type`) um cliente de ecommerce irá usar, com base em outras características da transação.

Para isso, usaremos um dataset público de ecommerce da Olist, disponível no Kaggle. Este é um problema de **classificação multiclasse**, pois a forma de pagamento pode ser `credit_card`, `boleto`, `voucher`, ou `debit_card`.

Vamos passar por todas as etapas de um projeto de ML:

1.  Carregar e explorar os dados.
2.  Realizar a engenharia de features para preparar os dados para o modelo.
3.  Construir um `Pipeline` de ML.
4.  Treinar e avaliar um modelo de `DecisionTreeClassifier` (Árvore de Decisão).

### 2. Preparando o Dataset

Primeiro, precisamos baixar o dataset do Kaggle. Usaremos o dataset "Olist E-Commerce" e, especificamente, o arquivo `olist_order_payments_dataset.csv`.

**Passo 1: Baixar o Dataset**

Você pode baixar o arquivo diretamente da página do dataset no Kaggle:
[https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Após baixar, faça o upload do arquivo `olist_order_payments_dataset.csv` para o seu ambiente Google Colab.

**Passo 2: Carregar e Explorar os Dados**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CursoHadoop_LabML").getOrCreate()

# Carregar o dataset de pagamentos
payments_df = spark.read.csv("olist_order_payments_dataset.csv", header=True, inferSchema=True)

# Explorar os dados
print("Schema do Dataset de Pagamentos:")
payments_df.printSchema()

print("\nExibindo algumas linhas:")
payments_df.show(5)

# Ver a distribuição das formas de pagamento (nossa variável alvo)
print("\nDistribuição das Formas de Pagamento:")
payments_df.groupBy("payment_type").count().show()
```

### 3. Engenharia de Features

Nosso objetivo é prever `payment_type`. Os algoritmos de MLlib precisam que a variável alvo (label) e as features (características) estejam em formato numérico.

Nosso plano de engenharia de features será:

1.  **`StringIndexer` para a Variável Alvo**: Converter a coluna `payment_type` (string) em uma coluna numérica chamada `label`. Esta é uma exigência da MLlib.
2.  **`VectorAssembler` para as Features**: Agrupar nossas features numéricas (`payment_sequential`, `payment_installments`, `payment_value`) em uma única coluna de vetor chamada `features`.

```python
from pyspark.ml.feature import StringIndexer, VectorAssembler

# Etapa 1: Converter a coluna alvo (string) para numérica (label)
# O StringIndexer atribui um índice numérico a cada categoria de string.
label_indexer = StringIndexer(inputCol="payment_type", outputCol="label")

# Etapa 2: Agrupar as colunas de features em um único vetor
# Nossas features são as colunas que usaremos para fazer a previsão.
feature_cols = ["payment_sequential", "payment_installments", "payment_value"]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
```

### 4. Construindo o Pipeline e o Modelo

Agora que temos nossos estágios de pré-processamento, vamos definir nosso modelo e montar o `Pipeline`.

*   **Modelo**: Usaremos um `DecisionTreeClassifier`, um modelo simples e interpretável, ótimo para começar.
*   **Pipeline**: Nosso pipeline terá 3 estágios: o `label_indexer`, o `vector_assembler`, e o `classifier`.

```python
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline

# Etapa 3: Definir o modelo (Estimator)
# labelCol="label" e featuresCol="features" são os nomes padrão, mas é bom ser explícito.
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")

# Etapa 4: Montar o Pipeline
# O pipeline define a sequência de operações do nosso fluxo de trabalho.
pipeline = Pipeline(stages=[label_indexer, vector_assembler, dt])
```

### 5. Treinamento e Avaliação

Com o pipeline definido, o próximo passo é dividir nossos dados em um conjunto de treinamento e um de teste. Treinaremos o modelo nos dados de treinamento e avaliaremos sua performance nos dados de teste, que ele nunca viu antes.

```python
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Dividir os dados: 80% para treinamento, 20% para teste
(training_data, test_data) = payments_df.randomSplit([0.8, 0.2], seed=42)

print(f"Registros de Treinamento: {training_data.count()}")
print(f"Registros de Teste: {test_data.count()}")

# Treinar o pipeline
# O método fit() executa todos os estágios do pipeline nos dados de treinamento.
print("\nIniciando o treinamento do modelo...")
pipeline_model = pipeline.fit(training_data)
print("Treinamento concluído!")

# Fazer previsões nos dados de teste
# O método transform() aplica o pipeline treinado para gerar previsões.
predictions_df = pipeline_model.transform(test_data)

print("\nExibindo as previsões:")
# Note as novas colunas: label, features, rawPrediction, probability, e prediction
predictions_df.select("payment_type", "label", "prediction", "probability").show(10)

# Avaliar o modelo
# Usaremos a métrica "accuracy" (acurácia) para avaliar.
# A acurácia mede a porcentagem de previsões corretas.
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")

accuracy = evaluator.evaluate(predictions_df)
print(f"\nAcurácia do modelo no conjunto de teste: {accuracy:.2%}")
```

### 6. Conclusão

Parabéns! Você construiu, treinou e avaliou seu primeiro modelo de machine learning com PySpark MLlib. Embora a acurácia possa não ser perfeita, o objetivo deste laboratório foi entender o processo e o paradigma da API `pyspark.ml`.

Você aprendeu a:

*   Preparar dados para machine learning usando `StringIndexer` e `VectorAssembler`.
*   Definir um `Estimator` (o modelo) e encadear tudo em um `Pipeline`.
*   Treinar o pipeline com `fit()` e fazer previsões com `transform()`.
*   Avaliar a performance do modelo usando um `Evaluator`.

Este fluxo de trabalho de Pipeline é a base para a construção de modelos de machine learning muito mais complexos e robustos no Spark. Nos próximos laboratórios, poderemos explorar outros algoritmos, técnicas de engenharia de features mais avançadas e métodos para otimizar a performance do modelo, como cross-validation e hyperparameter tuning.
# Módulo 5: Processamento de Dados em Tempo Real com Structured Streaming

## Capítulo 1: Introdução ao Structured Streaming

### 1.1. O que é Structured Streaming?

**Structured Streaming** é o motor de processamento de fluxos de dados (streaming) do Apache Spark, construído sobre a API de DataFrames e o motor Spark SQL. Ele foi projetado para tornar o desenvolvimento de aplicações de streaming mais simples, robusto e tolerante a falhas.

A ideia central e revolucionária do Structured Streaming é tratar um fluxo de dados em tempo real como uma **tabela que é continuamente atualizada**. Cada novo dado que chega ao fluxo é como uma nova linha sendo anexada a essa tabela infinita.

Isso permite que você aplique as mesmas operações que já conhece da API de DataFrames (como `select`, `filter`, `groupBy`, `join`) em um fluxo de dados. O Spark se encarrega de executar essa consulta de forma incremental e contínua, atualizando o resultado à medida que novos dados chegam.

> "Structured Streaming é um motor de processamento de fluxo escalável e tolerante a falhas, construído sobre o motor Spark SQL. Você pode expressar sua computação de streaming da mesma forma que expressaria uma computação em lote em dados estáticos." - Documentação Oficial do Apache Spark

### 1.2. O Modelo de Programação

O modelo de programação do Structured Streaming é surpreendentemente simples:

1.  **Definir uma Fonte de Entrada (Input Source)**: Você começa criando um DataFrame de streaming que representa o fluxo de dados de entrada. O Spark possui conectores para diversas fontes, como Apache Kafka, sistemas de arquivos (lendo novos arquivos em um diretório), sockets de rede, etc.

    ```python
    # Exemplo: Lendo um fluxo de arquivos CSV de um diretório
    input_df = spark.readStream \
        .schema(some_schema) \
        .csv("/path/to/input/dir")
    ```

2.  **Definir a Consulta (Query)**: Você aplica uma série de transformações (a "consulta") a este DataFrame de entrada, exatamente como faria com um DataFrame estático.

    ```python
    # Exemplo: Contando eventos por tipo
    result_df = input_df \
        .groupBy("event_type") \
        .count()
    ```

3.  **Definir um Coletor de Saída (Output Sink)**: Você especifica para onde o resultado da sua consulta deve ser enviado. Isso pode ser a memória (para depuração), o console, um sistema de arquivos (escrevendo em formatos como Parquet ou CSV) ou o Kafka.

4.  **Iniciar a Consulta (Start the Query)**: Você inicia o processamento do fluxo. O Spark começará a monitorar a fonte de entrada, processar os novos dados e atualizar o coletor de saída.

    ```python
    query = result_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    query.awaitTermination()
    ```

### 1.3. Modos de Saída (Output Modes)

O modo de saída define o que é escrito no coletor de saída a cada vez que o resultado é atualizado. Existem três modos principais:

*   **`append` (Anexar)**: (Padrão) Apenas as **novas linhas** adicionadas à tabela de resultados desde o último gatilho serão enviadas para o coletor. Isso é útil para consultas onde você não está agregando dados (ex: apenas filtrando).

*   **`complete` (Completo)**: A **tabela de resultados completa** será enviada para o coletor a cada gatilho. Isso é usado para consultas de agregação, onde você quer ver o resultado agregado atualizado a cada vez.

*   **`update` (Atualizar)**: Apenas as **linhas que foram atualizadas** na tabela de resultados desde o último gatilho serão enviadas para o coletor. É um meio-termo entre `append` e `complete`.

### 1.4. Tolerância a Falhas

Uma das características mais importantes do Structured Streaming é sua garantia de processamento **exatamente uma vez (exactly-once)**. Isso significa que, mesmo que ocorram falhas nos nós do cluster, o Spark garante que cada registro do fluxo de entrada será processado exatamente uma vez, sem perda ou duplicação de dados.

Isso é alcançado através de duas técnicas:

1.  **Checkpoints**: O Spark periodicamente salva o estado da sua consulta (quais dados já foram processados, os resultados intermediários das agregações, etc.) em um armazenamento confiável (como HDFS ou S3). Se a aplicação falhar, ela pode ser reiniciada a partir do último checkpoint, continuando de onde parou.
2.  **Write-Ahead Logs (WALs)**: Antes de processar um lote de dados, o Spark escreve as informações sobre esse lote em um log. Somente após o processamento ser concluído com sucesso é que o log é atualizado. Isso evita que os dados sejam reprocessados em caso de falha.

### 1.5. Casos de Uso

O Structured Streaming é ideal para uma ampla gama de aplicações em tempo real:

*   **Monitoramento e Alertas**: Análise de logs de servidor ou métricas de aplicação em tempo real para detectar anomalias e enviar alertas.
*   **Análise de Dados de IoT**: Processamento de dados de sensores de dispositivos IoT para monitoramento de condições, manutenção preditiva, etc.
*   **Detecção de Fraudes**: Análise de transações financeiras ou de cliques em tempo real para identificar padrões fraudulentos.
*   **ETL em Tempo Real**: Limpeza, transformação e enriquecimento de dados à medida que chegam, antes de carregá-los em um data warehouse ou data lake.
*   **Personalização de Experiência do Usuário**: Análise do comportamento do usuário em um site ou aplicativo em tempo real para fornecer recomendações ou conteúdo personalizado.

No próximo laboratório, vamos colocar o Structured Streaming em prática, criando uma aplicação simples para simular e processar um fluxo de dados de vendas em tempo real.
# Módulo 5: Processamento de Dados em Tempo Real com Structured Streaming

## Laboratório: Simulando e Processando um Fluxo de Vendas em Tempo Real

### 1. Objetivo

Neste laboratório, vamos construir nossa primeira aplicação de streaming com o Structured Streaming. Como nem sempre temos um fluxo de dados real (como o Kafka) disponível em um ambiente de desenvolvimento, vamos **simular** um fluxo de dados usando o próprio Spark.

O cenário será o seguinte: vamos gerar arquivos CSV de vendas em um diretório a cada poucos segundos. Nossa aplicação de Structured Streaming irá monitorar esse diretório, processar os novos arquivos assim que eles aparecerem e calcular uma agregação em tempo real: o **faturamento total por categoria de produto**.

Este laboratório irá ensiná-lo a:

*   Usar uma fonte de streaming baseada em sistema de arquivos.
*   Aplicar transformações (`join`, `groupBy`) em um DataFrame de streaming.
*   Usar o modo de saída `complete` para exibir agregações atualizadas.
*   Escrever o resultado da consulta de streaming no console.

### 2. Preparando o Ambiente e os Dados

Primeiro, precisamos de um diretório para simular nosso fluxo de entrada e dos dados base para a junção.

```python
import os
import shutil

# Definir os caminhos para os diretórios de streaming
input_path = "/tmp/streaming_input_vendas"
checkpoint_path = "/tmp/streaming_checkpoint_vendas"

# Limpar os diretórios de execuções anteriores para garantir um começo limpo
if os.path.exists(input_path):
    shutil.rmtree(input_path)
if os.path.exists(checkpoint_path):
    shutil.rmtree(checkpoint_path)

# Criar o diretório de entrada
os.makedirs(input_path)

print(f"Diretório de entrada para o streaming criado em: {input_path}")
print(f"Diretório de checkpoint será criado em: {checkpoint_path}")

# Carregar o DataFrame de produtos, que usaremos para o join
# Este é um DataFrame estático
produtos_df = spark.read.csv("produtos.csv", header=True, inferSchema=True)
```

### 3. Definindo a Aplicação de Streaming

Agora, vamos construir nossa aplicação passo a passo, conforme o modelo de programação do Structured Streaming.

**Passo 1: Definir a Fonte de Entrada**

Vamos configurar o Spark para ler arquivos CSV do diretório `input_path` como um fluxo. Precisamos fornecer o schema dos dados, pois a inferência de schema não é suportada para fontes de streaming baseadas em arquivo.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Definir o schema para os dados de vendas que vamos ler
vendas_schema = StructType([
    StructField("venda_id", IntegerType(), True),
    StructField("data_venda", TimestampType(), True),
    StructField("cliente_id", IntegerType(), True),
    StructField("produto_id", IntegerType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("preco_total", DoubleType(), True)
])

# Criar o DataFrame de streaming
streaming_vendas_df = spark.readStream \
    .schema(vendas_schema) \
    .option("maxFilesPerTrigger", 1)  # Processar um arquivo por vez
    .csv(input_path)
```

**Passo 2: Definir a Consulta**

Nossa consulta irá juntar o fluxo de vendas com os dados estáticos de produtos e depois calcular a soma do `preco_total` por `categoria`.

```python
# Juntar o DataFrame de streaming com o DataFrame estático de produtos
streaming_vendas_com_categoria_df = streaming_vendas_df.join(
    produtos_df, 
    streaming_vendas_df.produto_id == produtos_df.produto_id, 
    "inner"
)

# Agrupar por categoria e calcular o faturamento total
faturamento_por_categoria_df = streaming_vendas_com_categoria_df.groupBy("categoria") \
    .sum("preco_total") \
    .withColumnRenamed("sum(preco_total)", "faturamento_total") \
    .orderBy("faturamento_total", ascending=False)
```

**Passo 3: Definir o Coletor de Saída e Iniciar a Consulta**

Vamos escrever o resultado da nossa agregação no console. Como é uma agregação, usaremos o modo de saída `complete` para ver a tabela de resultados inteira a cada atualização.

```python
# Escrever o resultado no console
query = faturamento_por_categoria_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

print("Aplicação de streaming iniciada! Monitorando o diretório de entrada...")
```

### 4. Simulando o Fluxo de Dados

Com a nossa consulta de streaming rodando, ela está agora esperando por novos arquivos no diretório `/tmp/streaming_input_vendas`. Vamos criar uma função para gerar pequenos arquivos CSV de vendas e salvá-los nesse diretório para simular um fluxo.

**Execute o código abaixo em uma célula separada** para que você possa executá-lo várias vezes e ver a saída da sua consulta de streaming sendo atualizada.

```python
import time

# Pegar algumas linhas do DataFrame de vendas original para usar como dados de amostra
vendas_df = spark.read.csv("vendas.csv", header=True, inferSchema=True)
sample_data = vendas_df.take(20) # Pegar as primeiras 20 vendas

# Função para gerar um novo arquivo de vendas
def gerar_novo_arquivo_vendas(file_index):
    # Selecionar um pequeno lote de dados de amostra
    start = (file_index * 5) % len(sample_data)
    end = start + 5
    batch_df = spark.createDataFrame(sample_data[start:end])
    
    # Nome do arquivo
    file_name = f"vendas_{int(time.time())}.csv"
    output_file_path = os.path.join(input_path, file_name)
    
    # Escrever o lote como um único arquivo CSV no diretório de entrada
    # O Spark escreve em um diretório, então precisamos mover o arquivo gerado
    temp_path = f"/tmp/temp_vendas_{file_index}"
    batch_df.coalesce(1).write.csv(temp_path, header=True, mode="overwrite")
    
    # Encontrar o arquivo CSV gerado e movê-lo para o diretório de entrada
    part_file = [f for f in os.listdir(os.path.join(temp_path)) if f.endswith(".csv")][0]
    shutil.move(os.path.join(temp_path, part_file), output_file_path)
    shutil.rmtree(temp_path)
    
    print(f"✔️ Novo arquivo de vendas gerado: {output_file_path}")

# Gerar alguns arquivos para iniciar
for i in range(3):
    gerar_novo_arquivo_vendas(i)
    time.sleep(5) # Esperar um pouco entre os arquivos
```

### 5. Observando os Resultados

Volte para a célula onde a consulta de streaming está rodando. Você verá que, a cada novo arquivo CSV que é salvo no diretório de entrada, o Spark o processa e atualiza a tabela de faturamento por categoria no console. A saída será algo como:

```
-------------------------------------------
Batch: 0
-------------------------------------------
+---------------+------------------+
|categoria      |faturamento_total |
+---------------+------------------+
|Eletrônicos    |2848.5            |
|Brinquedos     |42.33             |
|Esportes       |765.15            |
+---------------+------------------+

-------------------------------------------
Batch: 1
-------------------------------------------
+---------------+------------------+
|categoria      |faturamento_total |
+---------------+------------------+
|Casa e Cozinha |3461.05           |
|Roupas         |2852.15           |
|Eletrônicos    |2848.5            |
|Ferramentas    |535.83            |
|Esportes       |765.15            |
|Brinquedos     |42.33             |
+---------------+------------------+
```

### 6. Parando a Consulta

Para parar a execução da consulta de streaming, você pode usar o método `stop()`.

```python
# Para parar a consulta
query.stop()
print("Aplicação de streaming parada.")
```

### 7. Conclusão

Neste laboratório, você construiu sua primeira aplicação de ponta a ponta com o Structured Streaming. Você aprendeu a ler um fluxo de dados de um sistema de arquivos, aplicar transformações complexas como joins e agregações, e exibir o resultado em tempo real no console.

O mais importante é que você viu como a API do Structured Streaming permite reutilizar todo o seu conhecimento sobre a API de DataFrames para processar dados em tempo real, tornando o desenvolvimento de aplicações de streaming muito mais simples e intuitivo.
# Módulo 6: Otimização e Performance no Spark

## Capítulo 1: Conceitos Fundamentais de Otimização

### 1.1. Por que a Otimização é Importante?

Escrever código Spark que funciona é apenas o primeiro passo. Em um ambiente de Big Data, escrever código que funciona de forma **eficiente** é crucial. Uma consulta mal otimizada pode levar horas para ser executada, consumir recursos excessivos do cluster e aumentar os custos operacionais. Por outro lado, uma consulta bem otimizada pode ser executada em minutos, usando os recursos de forma eficiente.

O Spark já faz um trabalho fantástico de otimização automática através do Catalyst Optimizer, mas há várias técnicas e configurações que você, como desenvolvedor, pode aplicar para extrair o máximo de performance da sua aplicação.

Neste módulo, vamos explorar os conceitos mais importantes de otimização no Spark, focando em particionamento, cache e a análise da Spark UI.

### 1.2. Particionamento: A Base da Paralelização

O **particionamento** é o conceito mais fundamental para entender a performance no Spark. Um DataFrame não é uma única unidade de dados; ele é dividido em várias partes menores chamadas **partições**. Cada partição é uma coleção de linhas que reside em um nó executor no cluster.

O número de partições determina o **grau de paralelismo** da sua aplicação. Se o seu DataFrame tem 100 partições, o Spark pode, teoricamente, executar 100 tarefas em paralelo para processar esses dados (assumindo que o cluster tenha recursos suficientes).

**Como o Spark decide o número de partições?**

*   **Na Leitura**: Ao ler dados de um sistema de arquivos como HDFS ou S3, o Spark geralmente cria uma partição para cada bloco do arquivo (por exemplo, um bloco de 128 MB).
*   **Após um Shuffle**: Operações como `groupBy`, `join`, e `repartition` causam um **shuffle**, que redistribui os dados. Por padrão, o número de partições após um shuffle é controlado pela configuração `spark.sql.shuffle.partitions` (o padrão é 200).

**O Dilema do Particionamento:**

*   **Muitas Partições Pequenas**: Causa uma sobrecarga no Driver, que precisa gerenciar um grande número de tarefas. Cada tarefa tem um pequeno custo de inicialização, e a soma desses custos pode se tornar significativa.
*   **Poucas Partições Grandes**: Leva a um baixo paralelismo. Se você tem apenas 4 partições, no máximo 4 tarefas podem ser executadas ao mesmo tempo, deixando o resto do cluster ocioso. Além disso, partições muito grandes podem causar problemas de memória nos executores.

O objetivo é encontrar um equilíbrio: ter partições suficientes para maximizar o paralelismo, mas não tantas a ponto de sobrecarregar o sistema. Uma boa regra geral é ter de 2 a 4 partições por core de CPU no seu cluster.

### 1.3. O Shuffle: O Grande Vilão da Performance

O **shuffle** é o processo de redistribuir os dados entre as partições. Ele é necessário para operações que precisam ver todos os valores de uma mesma chave para produzir um resultado, como `groupBy` (para agrupar todas as linhas com a mesma chave) e `join` (para juntar linhas com a mesma chave de junção).

O shuffle é uma operação extremamente custosa por duas razões:

1.  **I/O de Rede**: Envolve a transferência de grandes volumes de dados pela rede entre os executores.
2.  **I/O de Disco**: Os dados são primeiro escritos em disco pelos executores de origem (na fase de *map*) e depois lidos do disco pelos executadores de destino (na fase de *reduce*).

**Minimizar o número de shuffles é a otimização mais importante que você pode fazer em uma aplicação Spark.**

### 1.4. Cache e Persistência: Evitando Recálculos

Por padrão, o Spark reavalia um DataFrame e todas as suas transformações a partir da fonte original toda vez que uma ação é chamada. Se você usa o mesmo DataFrame em múltiplas ações, isso pode ser muito ineficiente.

Para evitar esse recálculo, você pode **persistir** um DataFrame em memória (e/ou em disco). Quando você persiste um DataFrame, o Spark armazena suas partições na primeira vez que ele é calculado. Nas vezes seguintes que você usar esse DataFrame, o Spark simplesmente lerá as partições da memória, o que é ordens de magnitude mais rápido do que recalcular tudo.

Os dois principais métodos para isso são:

*   **`cache()`**: É um atalho para `persist(StorageLevel.MEMORY_ONLY)`. Ele armazena as partições do DataFrame na memória da JVM dos executores.
*   **`persist(storage_level)`**: Oferece mais controle sobre como os dados são armazenados. Você pode escolher diferentes níveis de armazenamento, como:
    *   `MEMORY_ONLY`: Apenas memória (padrão do `cache()`).
    *   `MEMORY_AND_DISK`: Armazena na memória. Se não houver espaço suficiente, as partições que não couberem são armazenadas em disco.
    *   `DISK_ONLY`: Armazena apenas em disco.

**Quando usar `cache()`?**

Use `cache()` (ou `persist()`) em DataFrames que são usados múltiplas vezes em sua aplicação, especialmente se eles são o resultado de transformações caras (como joins ou agregações).

```python
# resultado_intermediario_df é o resultado de um join caro
resultado_intermediario_df = df1.join(df2, "id")

# Persistimos o resultado em memória
resultado_intermediario_df.cache()

# Primeira ação: dispara o cálculo e o cache
print(f"Contagem: {resultado_intermediario_df.count()}")

# Segunda ação: reutiliza os dados cacheados, muito mais rápido
resultado_intermediario_df.filter("coluna > 10").show()
```

### 1.5. A Spark UI: Sua Ferramenta de Diagnóstico

A **Spark UI** é uma interface web que fornece uma visão detalhada sobre sua aplicação Spark. É a ferramenta mais importante para entender o que está acontecendo por baixo dos panos, diagnosticar problemas de performance e validar suas otimizações.

Na Spark UI, você pode ver:

*   **Jobs**: As ações que dispararam a execução.
*   **Stages**: Os estágios em que cada job foi dividido (separados por shuffles).
*   **Tasks**: As tarefas individuais executadas para cada estágio.
*   **O plano de execução SQL**: O plano lógico e físico gerado pelo Catalyst Optimizer.
*   **Informações sobre armazenamento**: Quais DataFrames estão em cache e quanto espaço estão usando.

No próximo laboratório, vamos aplicar esses conceitos na prática e usar a Spark UI para observar o impacto de nossas otimizações.
# Módulo 6: Otimização e Performance no Spark

## Laboratório: Otimizando uma Análise de Vendas

### 1. Objetivo

Neste laboratório, vamos colocar em prática os conceitos de otimização que aprendemos. Vamos escrever uma consulta que, intencionalmente, não é otimizada e, em seguida, aplicar técnicas de **cache** e **reparticionamento** para melhorar drasticamente sua performance.

O mais importante é que vamos aprender a usar a **Spark UI** para **diagnosticar** os gargalos de performance e **verificar** o impacto de nossas otimizações. No Google Colab, a Spark UI é acessível através de um túnel `ngrok`.

O cenário será uma análise que envolve múltiplas ações sobre um DataFrame resultante de um join caro.

### 2. Configurando a Spark UI no Google Colab

Para acessar a Spark UI no Colab, precisamos expor a porta em que ela roda (porta 4040) para a internet. Faremos isso usando o `ngrok`.

```python
# Instalar o pyngrok
!pip -q install pyngrok

from pyngrok import ngrok

# Abrir um túnel público para a porta 4040 (porta padrão da Spark UI)
# Substitua <SEU_AUTHTOKEN> pelo seu token do ngrok (obtenha em https://dashboard.ngrok.com/get-started/your-authtoken)
# Se não tiver um token, pode funcionar, mas com limitações.
ngrok.set_auth_token("<SEU_AUTHTOKEN>")
public_url = ngrok.connect(4040)

print(f"Spark UI rodando em: {public_url}")
```

**Como usar:** Após iniciar sua `SparkSession`, clique no link gerado pelo `ngrok`. Isso abrirá a Spark UI em uma nova aba. Mantenha essa aba aberta para acompanhar a execução das suas consultas.

### 3. Cenário de Análise: Versão Não Otimizada

Vamos realizar uma análise que envolve juntar os DataFrames de vendas e produtos e, em seguida, executar três ações diferentes sobre o resultado.

```python
from pyspark.sql import SparkSession

# Iniciar a SparkSession
spark = SparkSession.builder.master("local[*]").appName("CursoHadoop_LabOtimizacao").getOrCreate()

# Carregar os dados
vendas_df = spark.read.csv("vendas.csv", header=True, inferSchema=True)
produtos_df = spark.read.csv("produtos.csv", header=True, inferSchema=True)

# Juntar os dois DataFrames. Esta é uma operação cara que envolve um shuffle.
vendas_produtos_df = vendas_df.join(produtos_df, "produto_id", "inner")

# --- VERSÃO NÃO OTIMIZADA ---

# Ação 1: Contar o número total de registros
print("Executando Ação 1...")
count = vendas_produtos_df.count()
print(f"Total de registros: {count}")

# Ação 2: Calcular o faturamento total por categoria
print("\nExecutando Ação 2...")
vendas_produtos_df.groupBy("categoria").sum("preco_total").show()

# Ação 3: Encontrar os 5 produtos com maior quantidade vendida
print("\nExecutando Ação 3...")
vendas_produtos_df.groupBy("nome_produto").sum("quantidade").orderBy("sum(quantidade)", ascending=False).show(5)
```

**Análise na Spark UI (Versão Não Otimizada):**

1.  Vá para a aba "Jobs" na Spark UI. Você verá **três jobs separados**, um para cada ação (`count`, `show`, `show`).
2.  Clique em um dos jobs e depois no estágio que menciona o `join`. Você verá que o join e o shuffle associado a ele foram **executados três vezes**, uma para cada job.
3.  Isso é extremamente ineficiente. Estamos refazendo a operação de join, que é a mais cara, a cada ação.

### 4. Otimizando com `cache()`

Agora, vamos aplicar a otimização mais simples e eficaz para este cenário: `cache()`. Vamos persistir o resultado do join em memória após a primeira vez que ele for calculado.

```python
# Juntar os dois DataFrames
vendas_produtos_df = vendas_df.join(produtos_df, "produto_id", "inner")

# --- VERSÃO OTIMIZADA COM CACHE ---

# Colocar o DataFrame resultante do join em cache
vendas_produtos_df.cache()

# Ação 1: Contar o número total de registros
# Esta ação irá disparar o cálculo do join e o armazenamento em cache.
print("Executando Ação 1 (com cache)...")
count = vendas_produtos_df.count()
print(f"Total de registros: {count}")

# Ação 2: Calcular o faturamento total por categoria
# Esta ação irá ler o DataFrame diretamente da memória, sem refazer o join.
print("\nExecutando Ação 2 (com cache)...")
vendas_produtos_df.groupBy("categoria").sum("preco_total").show()

# Ação 3: Encontrar os 5 produtos com maior quantidade vendida
# Esta ação também reutilizará o cache.
print("\nExecutando Ação 3 (com cache)...")
vendas_produtos_df.groupBy("nome_produto").sum("quantidade").orderBy("sum(quantidade)", ascending=False).show(5)

# Limpar o cache quando não for mais necessário
vendas_produtos_df.unpersist()
```

**Análise na Spark UI (Versão com Cache):**

1.  Vá para a aba "Jobs". Você ainda verá três jobs.
2.  Clique no **primeiro job** (`count`). Você verá que ele executa o join normalmente.
3.  Agora, clique no **segundo e terceiro jobs**. Você verá que os estágios que antes faziam o join agora são muito mais rápidos e têm uma nova etapa chamada "RDD Scan" ou "InMemoryTableScan". Isso indica que eles estão lendo os dados diretamente do cache, pulando o join e o shuffle.
4.  Vá para a aba "Storage". Você verá seu DataFrame `vendas_produtos_df` listado, com informações sobre quanto da memória ele está ocupando.

O tempo de execução do segundo e terceiro jobs será significativamente menor.

### 5. Otimizando o Particionamento com `repartition()`

Vamos supor que, após o join, nosso DataFrame `vendas_produtos_df` tenha muitas partições pequenas ou poucas partições grandes. Isso pode levar a um processamento ineficiente nas etapas seguintes. Podemos usar `repartition()` para otimizar o número de partições.

**Quando usar `repartition()`?**

*   Use `repartition()` para **aumentar ou diminuir** o número de partições. Ele sempre causa um **shuffle completo**, o que é caro, então use-o com moderação.
*   É útil após um filtro que reduz drasticamente o tamanho dos dados, deixando muitas partições vazias ou pequenas.
*   É útil antes de uma operação que se beneficia de um particionamento específico (que veremos em otimizações avançadas).

```python
# Verificar o número de partições após o join
num_particoes_inicial = vendas_produtos_df.rdd.getNumPartitions()
print(f"Número de partições após o join: {num_particoes_inicial}")

# Vamos supor que queremos trabalhar com 8 partições para as próximas etapas
# Em um cluster real, este número seria baseado no número de cores disponíveis
vendas_produtos_repart_df = vendas_produtos_df.repartition(8)

# Colocar em cache a versão reparticionada
vendas_produtos_repart_df.cache()

# Executar as ações novamente no DataFrame reparticionado
print("\nExecutando ações no DataFrame reparticionado...")
vendas_produtos_repart_df.count()
vendas_produtos_repart_df.groupBy("categoria").sum("preco_total").show()

vendas_produtos_repart_df.unpersist()
```

**Análise na Spark UI (com `repartition`):**

1.  Na aba "SQL / DataFrame", encontre a consulta que executou o `repartition`.
2.  Você verá um estágio de "Exchange" que corresponde ao shuffle causado pelo `repartition`.
3.  Os jobs subsequentes que usam o DataFrame `vendas_produtos_repart_df` agora serão executados com exatamente 8 tarefas em paralelo (uma para cada partição).

### 6. Conclusão

Neste laboratório, você aprendeu a usar a Spark UI como uma ferramenta de diagnóstico e aplicou duas das técnicas de otimização mais importantes:

*   **`cache()`**: Para evitar o recálculo de transformações caras quando um DataFrame é usado várias vezes.
*   **`repartition()`**: Para controlar o grau de paralelismo da sua aplicação, ajustando o número de partições.

Dominar a otimização é um processo contínuo de **medir, analisar e refatorar**. A Spark UI é sua melhor amiga nesse processo. Sempre que estiver lidando com grandes volumes de dados, use-a para entender como suas consultas estão sendo executadas e onde estão os gargalos.
