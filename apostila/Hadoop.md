# Apostila Completa: Ecossistema Hadoop

## Teoria, Prática e Laboratórios para Dominar o Big Data


**Data:** 10 de Fevereiro de 2026
**Versão:** 1.0

---

## Índice

### Parte I: Fundamentos do Hadoop
*   **Capítulo 1:** Introdução ao Big Data e ao Ecossistema Hadoop
*   **Capítulo 2:** Arquitetura do HDFS
*   **Capítulo 3:** O Framework MapReduce
*   **Capítulo 4:** YARN - O Gerenciador de Recursos do Hadoop

### Parte II: O Ecossistema de Ferramentas
*   **Capítulo 5:** Apache Hive - SQL para Hadoop
*   **Capítulo 6:** Apache Pig - Uma Plataforma para Análise de Big Data
*   **Capítulo 7:** Apache HBase - O Banco de Dados NoSQL do Hadoop
*   **Capítulo 8:** Apache Sqoop - A Ponte Entre o Hadoop e os Bancos de Dados Relacionais
*   **Capítulo 9:** Apache Flume - Ingestão de Dados de Streaming
*   **Capítulo 10:** Apache Oozie - Orquestração de Workflows no Hadoop

### Parte III: Laboratórios Práticos (Mão na Massa)
*   **Laboratório 1:** Introdução ao HDFS e Comandos Básicos
*   **Laboratório 2:** Seu Primeiro Job MapReduce com Hadoop Streaming
*   **Laboratório 3:** Análise de Dados com Apache Hive
*   **Laboratório 4:** Análise de Fluxo de Dados com Apache Pig
*   **Laboratório 5:** Ingestão de Dados com Sqoop e Flume

---

# Capítulo 1: Introdução ao Big Data e ao Ecossistema Hadoop

## 1.1 A Era do Big Data

Vivemos em uma era de geração de dados sem precedentes. A cada segundo, uma quantidade colossal de informações é criada a partir de uma miríade de fontes: redes sociais, transações financeiras, sensores de Internet das Coisas (IoT), registros de sistemas, vídeos, áudios e muito mais. Esse fenômeno é o que chamamos de **Big Data**.

No entanto, Big Data não se refere apenas ao volume. A definição clássica é baseada em múltiplos "Vs", que descrevem suas características fundamentais. Os mais conhecidos são os 5 Vs:

| Característica | Descrição |
| :--- | :--- |
| **Volume** | A quantidade de dados gerados é massiva, superando a capacidade de armazenamento e processamento de sistemas tradicionais. Falamos de terabytes, petabytes e até exabytes de informação. |
| **Velocidade** | Os dados são gerados e precisam ser processados em tempo real ou quase real. A velocidade com que os dados chegam exige infraestruturas capazes de alta vazão e baixa latência. |
| **Variedade** | Os dados vêm em formatos diversos. Podem ser **estruturados** (como bancos de dados relacionais), **semiestruturados** (como arquivos JSON ou XML) ou **não estruturados** (como textos, imagens e vídeos). |
| **Veracidade** | Refere-se à qualidade e confiabilidade dos dados. Com um volume tão grande e variado, garantir que os dados são precisos e confiáveis é um desafio significativo. |
| **Valor** | Talvez o "V" mais importante. O objetivo final de coletar e analisar Big Data é extrair insights valiosos que possam gerar inovação, otimizar processos, melhorar a tomada de decisão e criar novas oportunidades de negócio. |

> O desafio do Big Data não está apenas em armazenar essa imensa quantidade de dados, mas em processá-los de forma eficiente para extrair conhecimento útil em tempo hábil. As ferramentas tradicionais de Business Intelligence e bancos de dados relacionais, embora excelentes para dados estruturados e volumes moderados, mostraram-se insuficientes para lidar com a complexidade e a escala do Big Data.

## 1.2 O Surgimento do Apache Hadoop

Diante dos desafios impostos pelo Big Data, surgiu a necessidade de um novo paradigma de computação. Em 2006, inspirado por publicações do Google sobre seu sistema de arquivos distribuído [(]Google File System] (https://static.googleusercontent.com/media/research.google.com/pt-BR//archive/gfs-sosp2003.pdf) e seu modelo de processamento paralelo [MapReduce](https://static.googleusercontent.com/media/research.google.com/pt-BR//archive/mapreduce-osdi04.pdf), Doug Cutting e Mike Cafarella criaram o **(Apache Hadoop)[https://hadoop.apache.org/]**.

Hadoop é um framework de software de código aberto, escrito em Java, projetado para o **processamento distribuído de grandes conjuntos de dados** em clusters de computadores commodity (hardware de baixo custo). Em vez de depender de um único e caro supercomputador, o Hadoop utiliza o poder combinado de vários servidores menores, distribuindo tanto o armazenamento quanto o processamento.

Essa abordagem oferece duas vantagens cruciais:

1.  **Escalabilidade Horizontal:** Para aumentar a capacidade do sistema, basta adicionar mais máquinas ao cluster. Isso permite que o sistema cresça de forma linear e econômica para lidar com volumes de dados cada vez maiores.
2.  **Tolerância a Falhas:** O Hadoop foi projetado com a premissa de que falhas de hardware são comuns, não exceções. O framework detecta e lida automaticamente com falhas em máquinas ou discos, garantindo a alta disponibilidade dos dados e a continuidade do processamento sem intervenção manual.

## 1.3 Componentes Fundamentais do Hadoop

O núcleo do ecossistema Hadoop é composto por três componentes principais:

### 1.3.1 Hadoop Distributed File System (HDFS)

O **HDFS** é o sistema de arquivos distribuído do Hadoop, responsável pelo armazenamento dos dados. Ele divide arquivos grandes em blocos de tamanho fixo (tipicamente 128 MB ou 256 MB) e distribui esses blocos entre os nós do cluster. Para garantir a tolerância a falhas, cada bloco é replicado (geralmente 3 vezes por padrão) e armazenado em diferentes máquinas.

Sua arquitetura é do tipo mestre/escravo (*master/slave*), composta por:

-   **NameNode (Mestre):** O cérebro do HDFS. Ele gerencia o namespace do sistema de arquivos (a hierarquia de diretórios e arquivos) e armazena os metadados, como a localização de cada bloco de dados. Ele não armazena os dados reais.
-   **DataNodes (Escravos):** São os nós de trabalho que armazenam os blocos de dados em seus discos locais. Eles se comunicam periodicamente com o NameNode para reportar seu estado e os blocos que possuem.

### 1.3.2 MapReduce

**MapReduce** é o modelo de programação e o motor de processamento original do Hadoop. Ele permite o processamento paralelo de grandes volumes de dados de forma distribuída. Um trabalho MapReduce é dividido em duas fases principais:

-   **Fase Map (Mapeamento):** O conjunto de dados de entrada é dividido em partes menores e processado em paralelo pelos nós do cluster. A função `map` recebe pares de chave/valor e produz um conjunto de pares de chave/valor intermediários.
-   **Fase Reduce (Redução):** Os resultados intermediários da fase Map são agrupados por chave e processados pela função `reduce`. A função `reduce` agrega os valores associados a cada chave para produzir o resultado final.

O princípio fundamental do MapReduce é "mover o processamento para perto dos dados", e não o contrário. Em vez de mover gigabytes ou terabytes de dados pela rede até a aplicação, o Hadoop envia o código da aplicação para os nós onde os dados residem, minimizando o tráfego de rede e maximizando o throughput.

### 1.3.3 Yet Another Resource Negotiator (YARN)

Introduzido no Hadoop 2.0, o **YARN** é o gerenciador de recursos e agendador de tarefas do cluster. Ele desacoplou o gerenciamento de recursos do processamento de dados, que antes eram responsabilidades do MapReduce. Essa separação tornou o Hadoop uma plataforma de dados muito mais versátil, capaz de executar diferentes tipos de aplicações além do MapReduce, como Spark, Flink e Hive.

O YARN também possui uma arquitetura mestre/escravo:

-   **ResourceManager (Mestre):** Gerencia os recursos computacionais de todo o cluster e agenda as aplicações.
-   **NodeManager (Escravo):** Executa em cada nó do cluster, gerenciando os recursos (CPU, memória) daquela máquina e os contêineres onde as tarefas das aplicações são executadas.

Com o YARN, o Hadoop evoluiu de um simples motor de processamento em lote para uma plataforma de dados multifuncional, o que deu origem a um rico **ecossistema** de ferramentas e tecnologias que exploraremos nos próximos capítulos.

# Capítulo 2: Arquitetura do HDFS

## 2.1 Visão Geral do HDFS

O **Hadoop Distributed File System (HDFS)** é a espinha dorsal do ecossistema Hadoop, fornecendo uma solução de armazenamento robusta, escalável e tolerante a falhas, otimizada para grandes volumes de dados e acesso de alta performance. Como vimos no capítulo anterior, ele foi inspirado no Google File System (GFS) e é projetado para ser executado em clusters de hardware commodity.

As principais características do HDFS são:

-   **Armazenamento de Arquivos Muito Grandes:** É otimizado para armazenar arquivos que podem ter de gigabytes a petabytes de tamanho.
-   **Acesso Streaming:** O HDFS é projetado para acesso sequencial e em lote aos dados. A ênfase está em alta taxa de transferência (throughput) de dados, em vez de baixa latência de acesso. Isso o torna ideal para aplicações de processamento em lote como o MapReduce.
-   **Modelo de Coerência Simples:** O HDFS adota um modelo de acesso *write-once-read-many* (escreva uma vez, leia muitas). Um arquivo, uma vez criado e fechado, geralmente não é modificado. Isso simplifica a coerência dos dados e permite um acesso de alta velocidade.
-   **Tolerância a Falhas:** A detecção e recuperação de falhas são um objetivo central da arquitetura. O HDFS replica os dados em várias máquinas para garantir que a perda de um nó não resulte na perda de dados.
-   **Escalabilidade Linear:** A capacidade de armazenamento pode ser facilmente expandida simplesmente adicionando mais DataNodes ao cluster.

## 2.2 Arquitetura Master/Slave

O HDFS opera em uma arquitetura mestre/escravo (*master/slave*), que simplifica o design e o gerenciamento do sistema. A arquitetura é composta por dois tipos principais de nós:

### 2.2.1 NameNode (O Mestre)

O **NameNode** é o servidor mestre que gerencia todo o sistema de arquivos. Ele é o repositório central de todos os metadados do HDFS. Suas responsabilidades incluem:

-   **Gerenciamento do Namespace:** Mantém a árvore de diretórios e a hierarquia de arquivos. Todas as operações que modificam o namespace, como criar, renomear, mover ou deletar arquivos e diretórios, são executadas pelo NameNode.
-   **Mapeamento de Blocos:** Determina como os arquivos são divididos em blocos e onde esses blocos são armazenados nos DataNodes. Para cada arquivo, o NameNode mantém um mapa que associa os blocos do arquivo aos DataNodes que os contêm.
-   **Regulação do Acesso:** Controla o acesso dos clientes aos arquivos, aplicando permissões e políticas de segurança.
-   **Monitoramento de Saúde:** Recebe *heartbeats* (sinais de vida) e relatórios de blocos (*Block Reports*) dos DataNodes para garantir que eles estão operacionais e para manter um inventário atualizado dos blocos no cluster.

> É crucial entender que os dados do usuário **nunca** passam pelo NameNode. O cliente interage com o NameNode apenas para obter metadados e, em seguida, se comunica diretamente com os DataNodes para ler ou escrever os dados.

### 2.2.2 DataNodes (Os Escravos)

Os **DataNodes** são os nós de trabalho do HDFS. Normalmente, há um DataNode em execução em cada máquina do cluster. Suas principais funções são:

-   **Armazenamento de Blocos:** Armazenam e gerenciam os blocos de dados em discos locais, conforme instruído pelo NameNode.
-   **Servir Requisições:** Atendem às solicitações de leitura e escrita de blocos diretamente dos clientes.
-   **Gerenciamento de Blocos:** Realizam operações de baixo nível, como criação, exclusão e replicação de blocos, sob o comando do NameNode.
-   **Comunicação com o NameNode:** Enviam *heartbeats* regularmente para informar que estão vivos e *Block Reports* para listar os blocos que estão armazenando.

## 2.3 O Conceito de Blocos

O HDFS divide os arquivos em pedaços de tamanho fixo chamados **blocos**. O tamanho padrão de um bloco no Hadoop 2.x e superior é de **128 MB** (podendo ser configurado para 256 MB ou mais). Esse tamanho é significativamente maior do que o tamanho de bloco em sistemas de arquivos tradicionais (geralmente 4 KB ou 8 KB).

Essa abordagem de blocos grandes oferece várias vantagens:

-   **Redução de Metadados:** Um arquivo grande é representado por um número menor de blocos, o que significa que o NameNode precisa gerenciar menos metadados. Isso permite que o NameNode armazene os metadados de um número muito grande de arquivos em sua memória RAM, garantindo um acesso rápido.
-   **Otimização para Leitura Sequencial:** O tamanho grande do bloco permite que o HDFS transfira dados do disco com uma taxa de transferência muito alta, minimizando o custo de *seek time* (tempo de busca) do disco.

## 2.4 Replicação de Dados e Tolerância a Falhas

A tolerância a falhas é um pilar do HDFS e é alcançada através da **replicação de blocos**. Cada bloco de dados é replicado e armazenado em diferentes DataNodes no cluster. O **fator de replicação** padrão é **3**.

Isso significa que para cada bloco, existem três cópias em três máquinas diferentes. Se um DataNode falhar, o NameNode detecta a ausência de seus *heartbeats* e inicia o processo de re-replicação. Ele identifica os blocos que estavam no nó falho e instrui outros DataNodes a criarem novas cópias desses blocos a partir das réplicas existentes, mantendo assim o fator de replicação desejado.

### 2.4.1 Política de Posicionamento de Réplicas (Rack Awareness)

Para maximizar a resiliência, o HDFS não coloca as réplicas aleatoriamente. Ele utiliza uma política inteligente chamada **Rack Awareness** (Consciência de Rack). Um rack é um conjunto de máquinas que compartilham o mesmo switch de rede. A comunicação entre máquinas no mesmo rack é muito mais rápida do que entre máquinas em racks diferentes.

A política padrão de posicionamento para um fator de replicação 3 é:

1.  A **primeira réplica** é colocada em um DataNode no mesmo rack que o cliente que está escrevendo o dado (se possível).
2.  A **segunda réplica** é colocada em um DataNode em um **rack diferente**.
3.  A **terceira réplica** é colocada em um DataNode no **mesmo rack** que a segunda réplica, mas em um nó diferente.

Essa estratégia equilibra a necessidade de tolerância a falhas (se um rack inteiro falhar, os dados ainda estarão disponíveis no outro rack) e a performance de leitura/escrita (mantendo cópias em racks diferentes para otimizar a largura de banda da rede).

## 2.5 Processo de Leitura e Escrita de Dados

### 2.5.1 Escrita de um Arquivo

1.  O cliente informa ao NameNode que deseja criar um novo arquivo.
2.  O NameNode verifica as permissões e, se tudo estiver correto, cria um registro do novo arquivo em seu namespace.
3.  Para cada bloco do arquivo, o cliente solicita ao NameNode uma lista de DataNodes para armazenar as réplicas.
4.  O NameNode fornece ao cliente uma lista de DataNodes (geralmente 3) com base na política de *Rack Awareness*.
5.  O cliente escreve o bloco de dados diretamente para o primeiro DataNode da lista. Este DataNode, por sua vez, repassa o bloco para o segundo DataNode, que o repassa para o terceiro. Esse processo é chamado de **pipeline de replicação**.
6.  Uma vez que todas as réplicas são escritas, os DataNodes enviam uma confirmação de volta ao cliente.
7.  Quando todos os blocos do arquivo são escritos, o cliente informa ao NameNode para fechar o arquivo.

### 2.5.2 Leitura de um Arquivo

1.  O cliente informa ao NameNode qual arquivo deseja ler.
2.  O NameNode retorna a localização de todos os blocos do arquivo (ou seja, a lista de DataNodes que contêm as réplicas de cada bloco).
3.  Para cada bloco, o cliente contata o DataNode mais próximo que possui uma réplica daquele bloco e lê os dados diretamente dele.

Este capítulo forneceu uma visão detalhada da arquitetura do HDFS. No próximo, exploraremos o MapReduce, o modelo de processamento que opera sobre os dados armazenados no HDFS.
# Capítulo 3: O Framework MapReduce

## 3.1 O Paradigma do Processamento Paralelo

Como discutido anteriormente, um dos maiores desafios do Big Data é processar volumes massivos de dados de forma eficiente. A solução do Hadoop para este problema é o **MapReduce**, um modelo de programação e um framework de software para escrever aplicações que processam grandes conjuntos de dados em paralelo, de forma distribuída e tolerante a falhas.

A ideia central, inspirada em funções de programação funcional (`map` e `reduce`), é elegantemente simples e poderosa. Em vez de trazer os dados até a aplicação para serem processados, o MapReduce **move a lógica de processamento (o código) para perto dos dados**, que estão distribuídos pelo cluster HDFS. Isso minimiza drasticamente o tráfego de rede, que é frequentemente o gargalo em sistemas de computação distribuída.

> O MapReduce abstrai a complexidade da computação paralela. O desenvolvedor se concentra na lógica de negócio, definindo o que fazer nas fases de `map` e `reduce`, enquanto o framework se encarrega de todas as tarefas complexas: paralelização, distribuição de tarefas, balanceamento de carga, tolerância a falhas e comunicação entre os nós.

## 3.2 O Fluxo de um Trabalho MapReduce

Um trabalho (job) MapReduce opera sobre pares de **(chave, valor)**. Ele lê os dados de entrada como um conjunto de pares (chave, valor) e produz um conjunto de pares (chave, valor) como saída. Todo o processo é orquestrado pelo YARN e pode ser dividido nas seguintes etapas:

Dica de leitura: [O que é MapReduce](https://www.ibm.com/br-pt/think/topics/mapreduce)

1.  **Input (Entrada):** Os dados de entrada, geralmente armazenados em arquivos no HDFS, são divididos em *splits* (fragmentos). Cada *split* é processado por uma única tarefa Map.

2.  **Map Phase (Fase de Mapeamento):**
    -   Esta é a primeira fase de processamento, executada em paralelo em vários nós do cluster.
    -   A função `map`, definida pelo desenvolvedor, é aplicada a cada par (chave, valor) do *split* de entrada.
    -   O objetivo da fase Map é filtrar e transformar os dados, emitindo zero ou mais pares de (chave, valor) intermediários.
    -   **Exemplo:** Em uma contagem de palavras, a função `map` receberia uma linha de texto, a dividiria em palavras e emitiria um par `(palavra, 1)` para cada palavra encontrada.

3.  **Shuffle and Sort (Embaralhamento e Ordenação):**
    -   Esta é uma fase intermediária, gerenciada automaticamente pelo framework, que ocorre entre as fases Map e Reduce.
    -   Os resultados intermediários de todas as tarefas Map são coletados, ordenados e agrupados pela chave.
    -   Todos os valores associados à mesma chave são reunidos em uma única lista.
    -   **Exemplo:** Todos os pares `(palavra, 1)` para a mesma palavra (ex: "Hadoop") são agrupados, formando uma estrutura como `(Hadoop, [1, 1, 1, ...])`.

4.  **Reduce Phase (Fase de Redução):**
    -   A função `reduce`, também definida pelo desenvolvedor, é aplicada a cada chave única e sua lista de valores associados.
    -   O objetivo da fase Reduce é agregar, sumarizar ou transformar os dados para produzir o resultado final.
    -   **Exemplo:** A função `reduce` receberia `(Hadoop, [1, 1, 1, ...])` e somaria todos os valores da lista para emitir o resultado final `(Hadoop, 50)`.

5.  **Output (Saída):** Os pares (chave, valor) emitidos pela fase Reduce são escritos em um ou mais arquivos de saída no HDFS.

## 3.3 Anatomia de um Programa MapReduce (Java)

Embora seja possível escrever MapReduce em outras linguagens (usando Hadoop Streaming), o Java é a linguagem nativa. Um programa MapReduce básico consiste em três partes principais:

-   **A classe `Mapper`:** Contém a lógica da função `map`.
-   **A classe `Reducer`:** Contém a lógica da função `reduce`.
-   **A classe `Driver`:** Configura e inicia o trabalho MapReduce, especificando os caminhos de entrada/saída, as classes Mapper e Reducer, e os formatos de dados.

### Exemplo Clássico: Contagem de Palavras (WordCount)

Vamos ilustrar com o exemplo "Hello, World!" do MapReduce.

#### Mapper
```java
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one); 
        }
    }
}
```

#### Reducer
```java
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result); 
    }
}
```

#### Driver
```java
public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```
Dica: Você pode usar o python, ao invés do java, o exemplo está disponível na pasta [colab](https://github.com/gomesrocha/curso_hadoop/tree/main/colab).

## 3.4 Limitações e a Evolução do Ecossistema

Embora o MapReduce tenha sido revolucionário, ele possui algumas limitações:

-   **Verbosidade:** Escrever código MapReduce em Java pode ser prolixo e complexo para tarefas simples.
-   **Modelo Rígido:** O fluxo `map -> reduce` é adequado para muitos problemas, mas inflexível para outros, como processamento iterativo (comum em machine learning) ou interativo.
-   **Latência:** Por ser um modelo baseado em processamento em lote (batch) e por escrever resultados intermediários em disco, o MapReduce não é adequado para aplicações de baixa latência ou tempo real.

Essas limitações impulsionaram o desenvolvimento de novas ferramentas no ecossistema Hadoop, que oferecem abstrações de mais alto nível sobre o MapReduce ou modelos de processamento completamente diferentes. Ferramentas como **Hive** e **Pig** permitem que os desenvolvedores escrevam consultas em linguagens do tipo SQL ou scripts de fluxo de dados, que são então traduzidos para trabalhos MapReduce. Frameworks mais modernos, como o **Apache Spark**, superaram muitas das limitações de latência e flexibilidade do MapReduce, tornando-se o padrão de fato para muitas tarefas de Big Data.

Nos próximos capítulos, exploraremos essas ferramentas do ecossistema que tornam o trabalho com Hadoop mais produtivo e poderoso.
# Capítulo 4: YARN - O Gerenciador de Recursos do Hadoop

## 4.1 A Necessidade de um Novo Gerenciador

Na primeira versão do Hadoop (Hadoop 1.x), o framework MapReduce era responsável por duas tarefas distintas e cruciais: o **processamento de dados** e o **gerenciamento de recursos do cluster**. O componente central, chamado `JobTracker`, controlava tanto a execução dos trabalhos MapReduce quanto a alocação de recursos (slots de map e reduce) nos nós de trabalho, os `TaskTrackers`.

Essa arquitetura monolítica apresentava várias limitações significativas:

-   **Gargalo de Escalabilidade:** O `JobTracker` era um ponto único de falha e um gargalo de desempenho. Em clusters muito grandes, ele ficava sobrecarregado, limitando a escalabilidade do sistema.
-   **Inflexibilidade:** O cluster era rigidamente particionado em slots para tarefas Map e Reduce, levando a uma subutilização de recursos. Se um cluster estivesse executando apenas tarefas Map, os slots de Reduce ficavam ociosos.
-   **Acoplamento ao MapReduce:** O Hadoop era essencialmente sinônimo de MapReduce. Era extremamente difícil executar outros tipos de aplicações (que não fossem MapReduce) no mesmo cluster, impedindo a evolução do ecossistema.

Para superar esses desafios, o Hadoop 2.0 introduziu uma mudança arquitetônica fundamental: o **YARN (Yet Another Resource Negotiator)**.

## 4.2 A Arquitetura do YARN

O YARN desacoplou as duas principais responsabilidades do `JobTracker`: o gerenciamento de recursos e o gerenciamento do ciclo de vida da aplicação. Com o YARN, o Hadoop evoluiu de um framework de processamento em lote para uma plataforma de dados multifuncional, capaz de executar diversos tipos de cargas de trabalho (lote, interativo, streaming, grafos) no mesmo cluster, compartilhando os mesmos recursos.

O YARN também adota uma arquitetura mestre/escravo, mas com componentes mais especializados:


### 4.2.1 ResourceManager (O Mestre Global)

O **ResourceManager (RM)** é o mestre do YARN e o árbitro final dos recursos do cluster. Ele é responsável por gerenciar a alocação de recursos computacionais (CPU, memória) entre todas as aplicações em execução. O RM é composto por dois componentes principais:

-   **Scheduler (Agendador):** É um componente puramente de agendamento. Ele não monitora o estado da aplicação. Sua única função é alocar recursos para as aplicações com base em critérios como filas, capacidade e políticas de prioridade. O YARN oferece diferentes tipos de schedulers, como o *Capacity Scheduler* e o *Fair Scheduler*.
-   **ApplicationsManager (Gerenciador de Aplicações):** É responsável por aceitar submissões de trabalhos, negociar o primeiro contêiner para iniciar o `ApplicationMaster` de uma aplicação e reiniciar o `ApplicationMaster` em caso de falha.

### 4.2.2 NodeManager (O Escravo em Cada Nó)

O **NodeManager (NM)** é o agente do YARN que executa em cada nó de trabalho (DataNode) do cluster. Suas responsabilidades são:

-   **Gerenciamento do Nó:** Monitora o uso de recursos (CPU, memória, disco, rede) do nó em que está sendo executado.
-   **Gerenciamento de Contêineres:** É responsável por iniciar, monitorar e parar os **contêineres** no nó, conforme instruído pelo ResourceManager.
-   **Reporte de Status:** Comunica-se constantemente com o ResourceManager para enviar seu estado de saúde e o status dos contêineres que está gerenciando.

### 4.2.3 ApplicationMaster (O Mestre da Aplicação)

Quando um usuário submete uma aplicação (por exemplo, um trabalho MapReduce ou uma aplicação Spark), o YARN inicia um processo único e específico para essa aplicação, chamado **ApplicationMaster (AM)**. O AM é, na verdade, o primeiro contêiner que o YARN executa para uma aplicação.

O ApplicationMaster atua como um mestre para aquela aplicação específica. Suas funções são:

-   **Negociação de Recursos:** Negocia os recursos necessários (contêineres) com o ResourceManager.
-   **Coordenação de Tarefas:** Uma vez que os contêineres são alocados pelo RM, o AM os utiliza para executar as tarefas da aplicação, coordenando sua execução.
-   **Monitoramento de Tarefas:** Monitora o progresso de suas tarefas e as reinicia em caso de falha.

Essa arquitetura, com um ApplicationMaster por aplicação, é a chave para a escalabilidade do YARN. O ResourceManager não precisa mais se preocupar com os detalhes de cada tarefa individual, apenas com a alocação de recursos para os ApplicationMasters. Isso distribui a carga de gerenciamento e permite que o cluster Hadoop escale para dezenas de milhares de nós.

## 4.3 O Conceito de Contêiner

Um **contêiner** é uma abstração de uma alocação de recursos em um nó de trabalho. Ele representa uma quantidade específica de recursos (memória, CPU) que o ResourceManager aloca em um NodeManager para a execução de uma tarefa. Um contêiner não é uma máquina virtual completa, mas sim um conjunto de recursos reservados onde uma tarefa arbitrária pode ser executada.

## 4.4 O Fluxo de Execução de uma Aplicação no YARN

1.  **Submissão da Aplicação:** O cliente submete uma aplicação ao ResourceManager.
2.  **Lançamento do ApplicationMaster:** O ResourceManager aloca um contêiner em um NodeManager para iniciar o ApplicationMaster da aplicação.
3.  **Registro do ApplicationMaster:** O ApplicationMaster é iniciado e se registra no ResourceManager.
4.  **Solicitação de Recursos:** O ApplicationMaster analisa os requisitos da aplicação e solicita ao ResourceManager os contêineres necessários para executar suas tarefas.
5.  **Alocação de Contêineres:** O ResourceManager (através de seu Scheduler) aloca os contêineres nos NodeManagers disponíveis e informa ao ApplicationMaster sobre eles.
6.  **Lançamento das Tarefas:** O ApplicationMaster contata os NodeManagers para lançar as tarefas da aplicação dentro dos contêineres alocados.
7.  **Execução e Monitoramento:** As tarefas são executadas dentro de seus contêineres. O ApplicationMaster monitora seu progresso e o ResourceManager monitora o ApplicationMaster.
8.  **Conclusão:** Quando a aplicação termina, o ApplicationMaster cancela seu registro no ResourceManager, e todos os contêineres utilizados são liberados.

Com a introdução do YARN, o Hadoop se transformou em um sistema operacional para Big Data, permitindo que múltiplas e diversas aplicações de processamento de dados coexistam e compartilhem recursos de forma eficiente e segura. No próximo capítulo, começaremos a explorar as ferramentas do ecossistema que se beneficiam dessa arquitetura flexível, começando pelo Apache Hive. Hive e Pig.

# Capítulo 5: Apache Hive - SQL para Hadoop

## 5.1 A Necessidade de uma Abstração de Alto Nível

Embora o MapReduce seja um framework poderoso para o processamento de dados em larga escala, ele possui uma curva de aprendizado acentuada e sua programação em Java é, muitas vezes, verbosa e complexa para tarefas comuns de análise de dados. Desenvolver, testar e manter jobs MapReduce para consultas que seriam simples em um banco de dados relacional consome um tempo considerável.

Essa complexidade criou uma barreira para a adoção do Hadoop por parte de analistas de dados, cientistas de dados e desenvolvedores acostumados com a linguagem **SQL (Structured Query Language)**, a linguagem padrão para manipulação de dados em bancos de dados relacionais.

Para preencher essa lacuna, o Facebook desenvolveu o **Apache Hive**. O Hive é um projeto de código aberto que funciona como um sistema de *data warehouse* sobre o Hadoop. Sua principal função é fornecer uma interface semelhante ao SQL para consultar e analisar grandes conjuntos de dados armazenados no HDFS e em outras fontes de dados compatíveis com o Hadoop.

> O objetivo do Hive é democratizar o acesso aos dados no Hadoop, permitindo que usuários com conhecimento em SQL possam executar consultas ad-hoc, sumarizações e análises de dados sem a necessidade de escrever código MapReduce complexo em Java.

## 5.2 Arquitetura e Componentes do Hive

O Hive não é um banco de dados relacional. Ele não armazena os dados em um formato próprio, nem é projetado para processamento de transações online (OLTP). Em vez disso, ele impõe uma estrutura (um esquema) sobre os dados que já residem no HDFS e traduz as consultas SQL-like (chamadas de **HiveQL** ou **HQL**) em jobs MapReduce, Tez ou Spark que são executados no cluster YARN.


Os principais componentes do Hive são:

-   **Interface de Usuário (UI):** O Hive oferece várias maneiras para o usuário interagir com o sistema, incluindo uma linha de comando (CLI), uma interface web (HUE - Hadoop User Experience) e drivers JDBC/ODBC que permitem a conexão a partir de ferramentas de BI (Business Intelligence) como Tableau ou Power BI.
-   **Driver:** Recebe as consultas HQL da interface do usuário. Ele gerencia o ciclo de vida da consulta, otimiza o plano de execução e submete o trabalho para o cluster.
-   **Compiler (Compilador):** Analisa a consulta HQL, verifica a sintaxe e a semântica, e a converte em um plano de execução, que é um grafo acíclico direcionado (DAG) de etapas. Na maioria dos casos, essas etapas são jobs MapReduce.
-   **Optimizer (Otimizador):** Reescreve o plano de execução para melhorar a performance, aplicando regras de otimização como a ordem dos joins, por exemplo.
-   **Execution Engine (Motor de Execução):** Submete as tarefas (MapReduce, Tez, Spark) para o YARN para execução e monitora seu progresso.
-   **Metastore:** Este é um componente crítico. O **Metastore** armazena os metadados de todas as tabelas, partições e esquemas do Hive. Ele armazena informações como nomes de colunas, tipos de dados, localização dos arquivos no HDFS, formato dos dados (SerDe) e informações de particionamento. O Metastore geralmente utiliza um banco de dados relacional externo (como MySQL ou PostgreSQL) para persistir esses metadados.

## 5.3 O Metastore: O Coração do Hive

O Metastore é o que permite ao Hive impor uma estrutura relacional sobre dados não estruturados ou semiestruturados. Quando você cria uma tabela no Hive, você está, na verdade, definindo um esquema (metadados) que será armazenado no Metastore. Os dados em si permanecem nos arquivos do HDFS.

```sql
CREATE TABLE vendas (
    data STRING,
    produto STRING,
    quantidade INT,
    preco_unitario DOUBLE,
    cidade STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION 
'/user/hive/warehouse/vendas';
```

Neste exemplo, o Hive não move nem modifica os arquivos CSV localizados no diretório `/user/hive/warehouse/vendas`. Ele apenas armazena no Metastore que existe uma tabela chamada `vendas` e que, para ler seus dados, ele deve ir àquele local no HDFS e interpretar os arquivos de texto, sabendo que as colunas são separadas por vírgula.

## 5.4 Particionamento e Bucketing: Otimizando Consultas

O Hive oferece duas técnicas poderosas para otimizar a performance das consultas, reduzindo a quantidade de dados que precisam ser lidos do disco: **particionamento** e **bucketing**.

### 5.4.1 Particionamento

O **particionamento** divide uma tabela em partes menores com base nos valores de uma ou mais colunas. No HDFS, isso se traduz na criação de subdiretórios. Por exemplo, se a tabela `vendas` for particionada por `cidade`, o Hive criará um diretório para cada cidade:

-   `/user/hive/warehouse/vendas/cidade=Sao_Paulo/`
-   `/user/hive/warehouse/vendas/cidade=Rio_de_Janeiro/`
-   ...

Quando uma consulta utiliza a coluna de partição na cláusula `WHERE`, o Hive pode ignorar todos os diretórios (partições) que não correspondem ao filtro, lendo apenas os dados relevantes. Isso é conhecido como *partition pruning* e pode acelerar drasticamente as consultas.

### 5.4.2 Bucketing

O **bucketing** organiza os dados dentro de uma partição em um número fixo de arquivos (buckets). Ele funciona aplicando uma função de hash em uma ou mais colunas da tabela. O bucketing é útil para otimizar joins, permitindo que o Hive realize *map-side joins* mais eficientes, e também melhora a performance de amostragem de dados.

## 5.5 HiveQL: SQL para Big Data

O HiveQL é muito semelhante ao SQL padrão, suportando a maioria das operações DDL (Data Definition Language) e DML (Data Manipulation Language), como `CREATE TABLE`, `SELECT`, `GROUP BY`, `ORDER BY`, `JOIN`, etc. No entanto, existem algumas diferenças e extensões para lidar com a natureza distribuída e de grande escala dos dados.

```sql
-- Exemplo de consulta HiveQL para calcular o total de vendas por produto
SELECT
    produto,
    SUM(quantidade * preco_unitario) AS total_vendas
FROM vendas
WHERE cidade = 'Sao Paulo'
GROUP BY produto
ORDER BY total_vendas DESC;
```

Quando esta consulta é executada, o Hive a traduz em um ou mais jobs MapReduce (ou Tez/Spark) que são executados no cluster, e o resultado é retornado ao usuário.

O Hive foi uma ferramenta fundamental para a popularização do Hadoop, tornando a análise de Big Data acessível a um público muito mais amplo. No próximo capítulo, veremos o Apache Pig, outra ferramenta que oferece uma abstração de alto nível para programação MapReduce, mas com uma abordagem diferente, abordagem diferente, diferente, baseada em scripts de fluxo de dados.
_# Capítulo 6: Apache Pig - Uma Plataforma para Análise de Big Data

## 6.1 Por Que Outra Linguagem de Processamento de Dados?

Enquanto o Hive trouxe o poder do SQL para o Hadoop, tornando a análise de dados acessível para analistas e usuários de BI, ainda havia uma lacuna. O modelo declarativo do SQL é excelente para consultas e relatórios, mas pode ser limitado para tarefas mais complexas de **processamento de fluxo de dados (data flow)**, comuns em pipelines de ETL (Extração, Transformação e Carga) e em algoritmos de machine learning, que exigem uma série de transformações sequenciais nos dados.

Escrever essas lógicas complexas diretamente em MapReduce era, como já vimos, um processo árduo. Foi para preencher essa lacuna que o Yahoo! desenvolveu o **Apache Pig**.

Pig é uma plataforma de alto nível para a criação de programas MapReduce. Ele consiste em duas partes principais:

1.  **Pig Latin:** Uma linguagem de programação de fluxo de dados, procedural e de alto nível.
2.  **Um ambiente de execução:** Onde os scripts Pig Latin são executados. O Pig traduz os scripts Pig Latin em uma série de jobs MapReduce (ou Tez/Spark) que são executados no cluster Hadoop.

> A filosofia do Pig é fornecer uma linguagem que se situa entre a rigidez declarativa do SQL e a complexidade de baixo nível do MapReduce. Ele permite que os desenvolvedores se concentrem na lógica do fluxo de dados, em vez de se preocuparem com os detalhes da implementação do MapReduce.

## 6.2 A Linguagem Pig Latin

Um script Pig Latin é uma série de transformações aplicadas a um conjunto de dados. Cada transformação resulta em um novo conjunto de dados (uma *relação* ou *bag*). O fluxo de dados é explícito, tornando os scripts fáceis de ler, escrever e manter.

Os principais conceitos do Pig Latin são:

-   **Atom:** Um valor simples, como uma string ou um número (ex: 'Hadoop', 10).
-   **Tuple:** Uma sequência ordenada de campos, semelhante a uma linha em uma tabela (ex: ('Hadoop', 10)).
-   **Bag:** Uma coleção não ordenada de tuplas, semelhante a uma tabela, mas que pode conter tuplas duplicadas.
-   **Relation:** Uma Bag é também chamada de Relação.

### Estrutura de um Script Pig

Um script Pig geralmente segue este padrão:

1.  **LOAD:** Carrega os dados de uma fonte (geralmente HDFS) para uma relação.
2.  **Transformações:** Aplica uma série de operadores para transformar os dados (e.g., `FILTER`, `GROUP`, `JOIN`, `FOREACH`).
3.  **STORE / DUMP:** Salva o resultado final no HDFS (`STORE`) ou exibe na tela (`DUMP`).

## 6.3 Exemplo: Contagem de Palavras em Pig Latin

Vamos revisitar o nosso exemplo de contagem de palavras, desta vez implementado em Pig Latin. Compare a concisão e a clareza deste script com o código Java MapReduce do Capítulo 3.

```pig
-- Carrega as linhas do arquivo de entrada em uma relação chamada 'lines'.
-- Cada tupla em 'lines' terá um único campo chamado 'line'.
lines = LOAD 'input.txt' AS (line:chararray);

-- Quebra cada linha em palavras usando a função TOKENIZE.
-- O operador FOREACH aplica a transformação a cada tupla.
-- O resultado é uma relação 'words' onde cada tupla contém uma palavra.
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- Agrupa a relação 'words' pela coluna 'word'.
-- O resultado 'grouped' terá tuplas como ('Hadoop', {('Hadoop'), ('Hadoop'), ...})
grouped = GROUP words BY word;

-- Para cada grupo, conta o número de elementos na bag.
-- O resultado 'word_counts' terá tuplas como ('Hadoop', 50).
word_counts = FOREACH grouped GENERATE group AS word, COUNT(words) AS count;

-- Ordena os resultados em ordem decrescente de contagem.
ordered_counts = ORDER word_counts BY count DESC;

-- Exibe o resultado na tela.
DUMP ordered_counts;
```

## 6.4 Hive vs. Pig: Qual Usar?

Embora ambos sirvam para simplificar o trabalho com o Hadoop, Hive e Pig foram projetados com diferentes usuários e casos de uso em mente. A escolha entre eles depende da natureza da tarefa e do perfil do usuário.

| Característica | Apache Hive (HiveQL) | Apache Pig (Pig Latin) |
| :--- | :--- | :--- |
| **Paradigma** | Declarativo (O quê fazer) | Procedural / Fluxo de Dados (Como fazer) |
| **Linguagem** | SQL-like (HQL) | Linguagem de script, procedural |
| **Esquema** | Esquema na leitura (*Schema-on-read*), mas exige definição prévia na criação da tabela. | Esquema totalmente flexível, definido durante o processamento. |
| **Público-Alvo** | Analistas de Dados, Cientistas de Dados, Usuários de BI | Desenvolvedores, Engenheiros de Dados, Pesquisadores |
| **Caso de Uso Típico** | Consultas ad-hoc, relatórios, análise de dados exploratória, ETL simples. | Pipelines de ETL complexos, prototipagem de algoritmos, processamento de dados não estruturados. |
| **Estrutura** | Mais estruturado, opera sobre tabelas e partições. | Menos estruturado, opera sobre relações e fluxos de dados. |

Em resumo:

-   Use **Hive** se você e sua equipe já conhecem SQL e precisam realizar análises e relatórios sobre dados estruturados ou semiestruturados. É a ferramenta ideal para *data warehousing* e BI sobre Big Data.
-   Use **Pig** quando precisar criar pipelines de dados complexos, realizar transformações que não se encaixam bem no modelo SQL, ou trabalhar com dados não estruturados ou com esquemas que mudam com frequência. É uma ferramenta poderosa para engenheiros de dados que precisam de mais controle sobre o fluxo de processamento.

Ambas as ferramentas são peças valiosas do ecossistema Hadoop, cada uma resolvendo um conjunto diferente de problemas. No próximo capítulo, mergulharemos no Apache HBase, o banco de dados NoSQL do Hadoop, projetado para acesso de baixa latência a registros individuais em grande escala.
# Capítulo 7: Apache HBase - O Banco de Dados NoSQL do Hadoop

## 7.1 A Lacuna do Acesso em Tempo Real

Até agora, exploramos ferramentas do ecossistema Hadoop que são fundamentalmente projetadas para **processamento em lote (batch)**. O HDFS é otimizado para acesso sequencial e de alta vazão (throughput), enquanto o MapReduce, Hive e Pig são excelentes para executar análises complexas que leem grandes porções do dataset. A característica comum a todos eles é a **alta latência**: os trabalhos levam de segundos a horas para serem concluídos, o que é inaceitável para aplicações que precisam de acesso de leitura e escrita a registros individuais em tempo real.

Imagine um sistema que precisa exibir o perfil de um usuário instantaneamente ou registrar uma transação financeira em milissegundos. O HDFS e o MapReduce não foram feitos para isso. Essa necessidade de acesso aleatório e de baixa latência a grandes volumes de dados foi o que motivou a criação do **Apache HBase**.

HBase é um banco de dados **NoSQL**, distribuído, de código aberto, modelado a partir da publicação do Google sobre seu banco de dados, o **Bigtable**. Ele é executado sobre o HDFS e fornece acesso de leitura/escrita em tempo real e estritamente consistente por linha para petabytes de dados.

> O HBase preenche uma lacuna crítica no ecossistema Hadoop, combinando a escalabilidade e a tolerância a falhas do HDFS com a capacidade de acesso a dados de baixa latência de um banco de dados tradicional, mas em uma escala muito maior.

## 7.2 O Modelo de Dados do HBase

O HBase não é um banco de dados relacional. Ele é um banco de dados **orientado a colunas**, esparso, distribuído, persistente e multidimensional. Vamos desmembrar esses conceitos:

-   **Tabela:** Um conjunto de linhas.
-   **Linha (Row):** Cada linha é identificada unicamente por uma **Row Key**. As linhas em uma tabela HBase são ordenadas lexicograficamente pela Row Key. A escolha de uma boa Row Key é o aspecto mais importante do design de um esquema no HBase.
-   **Família de Colunas (Column Family):** Os dados em uma linha são agrupados em famílias de colunas. Todas as colunas de uma mesma família são armazenadas juntas no disco. As famílias de colunas devem ser definidas na criação da tabela.
-   **Qualificador de Coluna (Column Qualifier):** É o nome da coluna dentro de uma família. Ao contrário das famílias, os qualificadores de coluna não precisam ser predefinidos e podem ser adicionados dinamicamente a qualquer linha.
-   **Célula (Cell):** A interseção de uma Row Key, uma família de colunas e um qualificador de coluna. É onde o dado é efetivamente armazenado.
-   **Timestamp (Versão):** Cada célula pode conter múltiplas versões do mesmo dado, cada uma com um timestamp associado. O HBase armazena as versões em ordem decrescente de timestamp e, por padrão, retorna a mais recente. O número de versões a serem mantidas é configurável.


-   **Esparso:** Se uma linha não possui valor para uma determinada coluna, nada é armazenado, economizando espaço. Você pode ter milhões de colunas em uma família, mas apenas pagar o custo de armazenamento para aquelas que contêm dados.

## 7.3 Arquitetura do HBase

O HBase também utiliza uma arquitetura mestre/escravo, mas com componentes projetados para baixa latência.

### 7.3.1 HMaster (Mestre)

O **HMaster** é o servidor mestre do cluster HBase. Suas principais funções são:

-   **Coordenação:** Gerencia o cluster, incluindo o balanceamento de carga das regiões entre os RegionServers.
-   **Administração de Esquema:** Lida com operações de DDL (Data Definition Language), como criar e deletar tabelas e famílias de colunas.
-   **Atribuição de Regiões:** Atribui regiões aos RegionServers na inicialização e em caso de falha de um servidor.

Importante: O HMaster **não** está envolvido no caminho dos dados. Os clientes não se comunicam com o HMaster para operações de leitura ou escrita.

### 7.3.2 RegionServer (Escravo)

Os **RegionServers** são os nós de trabalho do HBase. Eles são responsáveis por:

-   **Servir Dados:** Gerenciam e servem os dados de um conjunto de **Regiões**.
-   **Operações de Dados:** Lidam com todas as requisições de leitura e escrita dos clientes para as regiões que gerenciam.
-   **Divisão de Regiões:** Quando uma região fica muito grande, o RegionServer a divide em duas novas regiões.

### 7.3.3 Região (Region)

Uma tabela HBase é dividida horizontalmente em pedaços chamados **Regiões**. Uma região é um intervalo contíguo de linhas, definido por uma chave de início e uma chave de fim. Cada região é servida por exatamente um RegionServer. A distribuição das regiões pelos RegionServers é o que permite a escalabilidade horizontal do HBase.

### 7.3.4 O Papel do ZooKeeper e do HDFS

-   **Apache ZooKeeper:** É um serviço de coordenação essencial para o HBase. Ele mantém o estado do cluster, como a localização das regiões e qual servidor é o HMaster ativo. Os clientes se conectam primeiro ao ZooKeeper para descobrir a localização do RegionServer que contém a Row Key que desejam acessar.
-   **HDFS:** O HBase utiliza o HDFS para a persistência dos dados. Todos os arquivos de dados (chamados HFiles) e os logs de transação (Write-Ahead Logs - WAL) são armazenados no HDFS, o que confere ao HBase a durabilidade e a tolerância a falhas inerentes ao sistema de arquivos do Hadoop.

## 7.4 Comparando HBase com Ferramentas do Ecossistema

| Característica | Apache HBase | HDFS | Apache Hive |
| :--- | :--- | :--- | :--- |
| **Tipo de Acesso** | Aleatório, em tempo real (leitura/escrita) | Sequencial, em lote | Em lote, via consultas SQL-like |
| **Latência** | Baixa (milissegundos) | Alta (segundos a minutos) | Alta (minutos a horas) |
| **Principal Uso** | Banco de dados operacional (OLTP), aplicações web | Armazenamento para processamento em lote | Data warehouse, análise de dados (OLAP) |
| **Unidade de Dados** | Células em uma tabela orientada a colunas | Blocos de arquivos | Tabelas com esquema definido |
| **Atualizações** | Suporte a atualizações e exclusões por célula | Arquivos são tipicamente imutáveis | Atualizações são complexas e ineficientes |

## 7.5 Quando Usar o HBase?

Use o HBase quando sua aplicação precisar de:

-   **Acesso de baixa latência:** Servir dados para um site ou aplicação em tempo real.
-   **Escalabilidade massiva:** Armazenar bilhões de linhas e milhões de colunas.
-   **Esquema flexível:** Lidar com dados semiestruturados onde o número de colunas pode variar entre as linhas.
-   **Consistência forte:** Garantir que uma operação de escrita seja imediatamente visível para leituras subsequentes na mesma linha.

Casos de uso comuns incluem armazenamento de perfis de usuário, catálogos de produtos, dados de séries temporais, dados de sensores de IoT e armazenamento de grafos de redes sociais.

No próximo capítulo, abordaremos o Sqoop e o Flume, duas ferramentas essenciais para mover dados para dentro e para fora do seu ecossistema Hadoop.
'''
# Capítulo 8: Apache Sqoop - A Ponte Entre o Hadoop e os Bancos de Dados Relacionais

## 8.1 O Desafio da Ingestão de Dados Estruturados

Um ecossistema Hadoop raramente existe de forma isolada. Na maioria das organizações, uma grande quantidade de dados valiosos e estruturados reside em **bancos de dados relacionais (RDBMS)**, como Oracle, MySQL, PostgreSQL e SQL Server. Esses sistemas são a espinha dorsal de muitas aplicações transacionais (OLTP) e sistemas legados.

Para realizar análises de Big Data de forma eficaz, é crucial conseguir combinar esses dados estruturados com os dados não estruturados e semiestruturados já presentes no HDFS. O desafio é: como mover, de forma eficiente e confiável, grandes volumes de dados entre o Hadoop e os sistemas RDBMS?

Fazer isso manualmente, escrevendo scripts customizados, é um processo propenso a erros, difícil de manter e que não escala bem. É para resolver exatamente este problema que o **Apache Sqoop** foi criado.

Sqoop (uma contração de "SQL-to-Hadoop") é uma ferramenta projetada para transferir dados em massa entre o Apache Hadoop e fontes de dados estruturadas, como bancos de dados relacionais.

> O Sqoop automatiza a maior parte do processo de importação e exportação de dados, tratando o processo como um job MapReduce. Ele busca os metadados do esquema do banco de dados e usa essa informação para gerar código MapReduce que realiza a transferência de dados de forma paralela e tolerante a falhas.

## 8.2 Como o Sqoop Funciona?

O Sqoop simplifica a transferência de dados ao traduzir a operação em um job MapReduce. O processo geral é o seguinte:

1.  **Introspecção:** O Sqoop se conecta ao banco de dados de origem e examina a tabela a ser importada, obtendo os metadados (nomes das colunas, tipos de dados, chave primária, etc.).
2.  **Geração de Código:** Com base nesses metadados, o Sqoop gera automaticamente uma classe Java que pode representar uma linha da tabela. Ele também gera o código do job MapReduce que irá orquestrar a transferência.
3.  **Execução Paralela (Importação):**
    -   O Sqoop divide a tarefa de importação entre várias tarefas Map. Por padrão, ele tenta dividir os dados com base na chave primária da tabela, criando *splits* (fatias) de dados para cada mapper processar.
    -   Cada tarefa Map executa uma consulta SQL (ex: `SELECT * FROM tabela WHERE id >= 1 AND id < 10000`) para buscar uma fatia dos dados do RDBMS.
    -   Os dados são então escritos no HDFS em um formato de arquivo especificado (como texto, Avro ou Parquet).
4.  **Execução Paralela (Exportação):**
    -   O processo inverso também é possível. O Sqoop pode pegar dados de arquivos no HDFS e inseri-los em uma tabela em um banco de dados relacional.
    -   Ele divide os arquivos de entrada e usa múltiplos mappers para executar instruções `INSERT` ou `UPDATE` em paralelo no banco de dados de destino.


## 8.3 Comandos e Casos de Uso

O Sqoop é operado principalmente via linha de comando, com dois comandos principais: `import` e `export`.

### 8.3.1 Importando Dados com `sqoop import`

Este é o caso de uso mais comum. Um comando de importação típico se parece com isto:

```bash
sqoop import \
    --connect jdbc:mysql://db.example.com/database \
    --username user \
    --password pass \
    --table vendas \
    --target-dir /user/hadoop/vendas \
    --m 4
```

Vamos analisar os parâmetros:

-   `--connect`: A string de conexão JDBC para o banco de dados.
-   `--username` / `--password`: As credenciais de acesso.
-   `--table`: A tabela a ser importada.
-   `--target-dir`: O diretório de destino no HDFS onde os dados serão salvos.
-   `--m 4`: O número de mappers a serem usados para a importação paralela (neste caso, 4).

O Sqoop também oferece dezenas de outras opções para controlar o processo, como:

-   `--where`: Para filtrar os dados a serem importados.
-   `--columns`: Para selecionar colunas específicas.
-   `--split-by`: Para especificar uma coluna a ser usada para dividir o trabalho entre os mappers.
-   `--as-parquetfile` ou `--as-avrodatafile`: Para salvar os dados em formatos de arquivo colunares e otimizados, em vez de texto simples.

### 8.3.2 Exportando Dados com `sqoop export`

O processo de exportação move os dados do HDFS de volta para um RDBMS. Isso é útil quando os resultados de uma análise no Hadoop precisam ser carregados em um sistema de BI ou em uma aplicação operacional.

```bash
sqoop export \
    --connect jdbc:mysql://db.example.com/database \
    --username user \
    --password pass \
    --table relatorio_vendas \
    --export-dir /user/hadoop/analise_resultado \
    --input-fields-terminated-by ','
```

-   `--table`: A tabela de destino no banco de dados.
-   `--export-dir`: O diretório no HDFS que contém os dados a serem exportados.
-   `--input-fields-terminated-by`: Especifica o delimitador dos campos nos arquivos de texto de entrada.

## 8.4 Sqoop vs. Flume: Qual a Diferença?

É comum que iniciantes confundam o Sqoop com outra ferramenta de ingestão de dados, o Apache Flume (que veremos no próximo capítulo). A principal diferença está na natureza dos dados que eles manipulam:

-   **Sqoop:** Projetado para **transferências em massa (bulk) de dados estruturados** de e para fontes de dados que suportam JDBC (principalmente RDBMS). É ideal para cargas em lote, programadas.
-   **Flume:** Projetado para **coletar, agregar e mover grandes volumes de dados de streaming (eventos)**, como logs de servidores, tweets ou dados de sensores. Ele é focado em ingestão contínua e em tempo real.

Em resumo, o Sqoop é a ferramenta ideal para integrar o seu data lake Hadoop com o mundo dos bancos de dados relacionais, permitindo uma visão de 360 graus dos seus dados. No próximo capítulo, vamos nos aprofundar no Apache Flume e entender como lidar com a ingestão de dados de eventos em tempo real.
'''
# Capítulo 9: Apache Flume - Ingestão de Dados de Streaming

## 9.1 O Desafio da Ingestão de Dados de Eventos

No capítulo anterior, vimos como o Apache Sqoop é eficiente para a transferência em massa de dados estruturados de bancos de dados relacionais para o HDFS. No entanto, uma grande parte do Big Data não é gerada em lotes, mas sim como um fluxo contínuo de eventos. Considere as seguintes fontes de dados:

-   **Logs de servidores web:** Cada clique, cada visualização de página, cada erro gera uma linha de log.
-   **Feeds de redes sociais:** Milhões de tweets, posts e curtidas são gerados a cada segundo.
-   **Dados de sensores (IoT):** Dispositivos em fábricas, carros ou residências enviam leituras contínuas.
-   **Logs de transações financeiras:** Cada operação com cartão de crédito gera um evento.

Coletar esses dados de streaming de forma confiável e em tempo real, a partir de centenas ou milhares de fontes, e entregá-los ao HDFS ou HBase para análise é um desafio complexo. É para isso que o **Apache Flume** foi projetado.

O Flume é um serviço distribuído, confiável e de alta disponibilidade para coletar, agregar e mover com eficiência grandes quantidades de dados de log e eventos de várias fontes para um repositório centralizado, como o HDFS.

> A principal filosofia do Flume é fornecer um sistema de ingestão de dados robusto e simples, baseado em um fluxo de dados configurável, que garante a entrega dos eventos mesmo em caso de falhas.

## 9.2 Arquitetura e Conceitos Fundamentais do Flume

A arquitetura do Flume é baseada em um conceito de fluxo de dados flexível. O componente central é o **Agente Flume (Flume Agent)**, que é um processo Java que hospeda os componentes que compõem o fluxo de dados. Os três componentes principais de um agente são:

-   **Source (Fonte):** O componente que recebe os dados de uma fonte externa (como um servidor web, um cliente de log ou um feed do Twitter) e os transforma em **Eventos Flume**. Um evento é a unidade básica de dados no Flume, consistindo em um *payload* (o corpo do dado, em bytes) e um conjunto opcional de *headers* (cabeçalhos, pares de chave-valor).
-   **Channel (Canal):** Um armazenamento temporário que atua como um buffer entre a Source e o Sink. A Source coloca os eventos no Channel, e o Sink os remove. O uso de um canal desacopla a fonte do destino, permitindo que eles operem em taxas diferentes. O Flume oferece diferentes tipos de canais, como o `MemoryChannel` (rápido, mas não persistente) e o `FileChannel` (persistente no disco, garantindo durabilidade em caso de falha do agente).
-   **Sink (Destino):** O componente que remove os eventos do Channel e os envia para o seu destino final. O destino pode ser o HDFS, o HBase, outro agente Flume (para criar fluxos de múltiplos saltos), ou outros sistemas de armazenamento.


## 9.3 Construindo Fluxos de Dados (Data Flows)

A verdadeira potência do Flume reside na sua capacidade de conectar múltiplos agentes para criar fluxos de dados complexos e distribuídos. É possível criar topologias para agregação, replicação e balanceamento de carga.

-   **Agregação:** Vários agentes em diferentes servidores podem enviar seus dados para um único agente centralizador, que agrega os eventos antes de escrevê-los no HDFS.
-   **Replicação (Fan-out):** Um único agente pode ser configurado para enviar o mesmo fluxo de eventos para múltiplos destinos (Sinks), por exemplo, para o HDFS para arquivamento e para o HBase para análise em tempo real.

## 9.4 Exemplo de Configuração de um Agente Flume

Um agente Flume é configurado através de um arquivo de propriedades simples. Vamos ver um exemplo que monitora um arquivo de log em um servidor web e envia os novos logs para o HDFS.

```properties
# Nomear os componentes do agente
agent1.sources = src1
agent1.channels = ch1
agent1.sinks = sink1

# Configurar a Source (Fonte)
# Usaremos uma fonte do tipo 'exec' que executa um comando (tail -F) para ler novas linhas de um arquivo.
agent1.sources.src1.type = exec
agent1.sources.src1.command = tail -F /var/log/apache2/access.log
agent1.sources.src1.channels = ch1

# Configurar o Channel (Canal)
# Usaremos um canal em memória para alta performance.
agent1.channels.ch1.type = memory
agent1.channels.ch1.capacity = 10000
agent1.channels.ch1.transactionCapacity = 1000

# Configurar o Sink (Destino)
# O destino será o HDFS.
agent1.sinks.sink1.type = hdfs
agent1.sinks.sink1.hdfs.path = hdfs://namenode:8020/flume/weblogs/%Y-%m-%d
agent1.sinks.sink1.hdfs.filePrefix = access_log
agent1.sinks.sink1.hdfs.fileType = DataStream
agent1.sinks.sink1.hdfs.rollInterval = 3600 # Cria um novo arquivo a cada hora
agent1.sinks.sink1.channel = ch1
```

Nesta configuração:

1.  A **Source** `src1` executa o comando `tail -F` para capturar continuamente novas linhas do arquivo de log do Apache.
2.  Os eventos são colocados no **Channel** `ch1`, que é um buffer em memória.
3.  O **Sink** `sink1` retira os eventos do canal e os escreve no HDFS, no diretório `/flume/weblogs/`, criando subdiretórios por data e rotacionando os arquivos a cada hora.

## 9.5 Garantias de Confiabilidade

O Flume foi projetado para ser confiável. A entrega de eventos entre a Source, o Channel e o Sink é **transacional**. A Source só remove o evento da sua origem depois que ele foi com sucesso armazenado no Channel. Da mesma forma, o Sink só remove o evento do Channel depois que ele foi com sucesso escrito no destino final.

O uso de um `FileChannel` oferece uma garantia ainda maior. Como os eventos são persistidos no disco local do agente, mesmo que o processo do agente falhe e seja reiniciado, os dados no canal não são perdidos e a entrega pode ser retomada.

O Flume é uma ferramenta indispensável para construir pipelines de ingestão de dados em tempo real para o Hadoop. Ele atua como a linha de frente do seu data lake, garantindo que os dados de eventos de alta velocidade sejam capturados de forma confiável e eficiente. No próximo capítulo, vamos explorar o Oozie, uma ferramenta para orquestrar e agendar os complexos fluxos de trabalho que são comuns em um ambiente de Big Data.
# Capítulo 10: Apache Oozie - Orquestração de Workflows no Hadoop

## 10.1 O Desafio da Orquestração de Tarefas

Nos capítulos anteriores, exploramos um rico conjunto de ferramentas do ecossistema Hadoop, cada uma especializada em uma tarefa: Sqoop para ingestão de dados de RDBMS, Flume para dados de streaming, Pig e Hive para transformação e análise, e HBase para acesso em tempo real. No mundo real, uma análise de Big Data raramente envolve a execução de uma única tarefa isolada. Em vez disso, ela consiste em um **pipeline de dados**, uma sequência de tarefas com dependências complexas entre si.

Considere um pipeline de análise diário típico:

1.  **02:00 AM:** Iniciar a ingestão de dados de vendas do dia anterior de um banco de dados Oracle usando **Sqoop**.
2.  **Paralelamente:** Iniciar a coleta de logs de cliques do servidor web do dia anterior usando **Flume**.
3.  **Após a conclusão de AMBAS as ingestões:** Executar um script **Pig** para limpar, enriquecer e juntar os dados de vendas com os logs de cliques.
4.  **Se o script Pig for bem-sucedido:** Executar uma consulta **Hive** para agregar os dados e gerar um relatório de sumarização.
5.  **Se a consulta Hive falhar:** Enviar um e-mail de alerta para a equipe de operações.
6.  **Se tudo for bem-sucedido:** Exportar o relatório final para um dashboard de BI usando **Sqoop**.

Gerenciar manualmente essa complexidade usando scripts de shell e `cron jobs` é uma tarefa árdua, frágil e que não escala. O que acontece se a ingestão do Sqoop demorar mais do que o esperado? Como garantir que o script Pig só comece depois que *ambas* as fontes de dados estiverem prontas? Como lidar com falhas e reexecuções parciais?

É para resolver esses problemas de orquestração que o **Apache Oozie** foi criado.

## 10.2 O que é o Apache Oozie?

Oozie é um sistema de agendamento de fluxos de trabalho (workflows) para gerenciar e coordenar jobs do Apache Hadoop. Ele permite que os usuários definam uma série de ações e suas dependências em um **Grafo Acíclico Dirigido (DAG)**, especificando a ordem e as condições para a execução das tarefas.

> O Oozie atua como o maestro do seu cluster Hadoop, garantindo que cada instrumento (Sqoop, Pig, Hive, etc.) toque na hora certa e em harmonia com os outros para executar a sinfonia da sua análise de dados.

Oozie é um serviço baseado em servidor que é executado no cluster e se integra nativamente com o YARN e o HDFS.

## 10.3 Conceitos Fundamentais do Oozie

O Oozie é construído em torno de três conceitos principais:

1.  **Workflow:** É o coração do Oozie. Um workflow é uma definição de um DAG que representa a lógica de controle e as ações a serem executadas. Ele define o fluxo de execução e as dependências entre as tarefas. Por exemplo, "execute a Ação A, e se ela for bem-sucedida, execute a Ação B; caso contrário, execute a Ação C".

2.  **Coordinator:** É uma camada acima do workflow que agenda a execução de workflows com base em gatilhos de **tempo** e/ou **disponibilidade de dados**. Por exemplo, um Coordinator pode ser configurado para "executar o workflow de análise diária todos os dias às 05:00 AM, mas somente se os diretórios de dados de entrada para aquele dia existirem no HDFS".

3.  **Bundle:** É a abstração de mais alto nível, que permite agrupar e gerenciar múltiplos Coordinators e workflows como uma única unidade lógica, simplificando o gerenciamento de pipelines de dados complexos.

## 10.4 Definindo um Workflow Oozie

Os workflows do Oozie são definidos em arquivos **XML** (por exemplo, `workflow.xml`). A estrutura XML descreve os nós do grafo e as transições entre eles.

Os principais tipos de nós em um workflow são:

-   **Nós de Controle (Control Nodes):** Definem o início e o fim do fluxo e controlam o caminho da execução. Incluem `start`, `end`, `kill`, `fork`, `join` e `decision`.
    -   `fork` e `join` são usados para executar ações em paralelo.
-   **Nós de Ação (Action Nodes):** Representam a execução de uma tarefa real. Oozie tem suporte nativo para uma vasta gama de ações, incluindo `hive`, `pig`, `sqoop`, `map-reduce`, `spark`, `shell` e `java`.

### Exemplo de um Workflow Simples

Este exemplo mostra um workflow que executa uma ação do Hive.

```xml
<workflow-app name="simple-hive-workflow" xmlns="uri:oozie:workflow:0.5">
    <start to="hive-action"/>

    <action name="hive-action">
        <hive xmlns="uri:oozie:hive-action:0.2">
            <job-tracker>${jobTracker}</job-tracker>
            <name-node>${nameNode}</name-node>
            <script>summarize.hql</script>
            <param>INPUT_DIR=${inputDir}</param>
            <param>OUTPUT_DIR=${outputDir}</param>
        </hive>
        <ok to="end"/>
        <error to="kill-job"/>
    </action>

    <kill name="kill-job">
        <message>A ação do Hive falhou: [${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>

    <end name="end"/>
</workflow-app>
```

Neste XML:
- O workflow começa no nó `start` e transita para a ação `hive-action`.
- A `hive-action` executa o script `summarize.hql`.
- Se a ação for bem-sucedida (`ok`), o workflow transita para o nó `end`.
- Se ocorrer um erro (`error`), ele transita para o nó `kill-job`, que termina o workflow e registra uma mensagem de erro.
- As variáveis como `${jobTracker}` e `${inputDir}` são parametrizadas e definidas em um arquivo de configuração separado.

## 10.5 O Poder dos Coordinators

Enquanto os workflows definem o *o quê* e o *como*, os Coordinators definem o *quando*. Um Coordinator, também definido em XML, especifica a frequência de execução e as dependências de dados.

```xml
<coordinator-app name="daily-sales-coord" 
                 frequency="${coord:days(1)}" 
                 start="2026-01-20T05:00Z" 
                 end="2027-01-01T05:00Z" 
                 timezone="UTC" 
                 xmlns="uri:oozie:coordinator:0.4">
    <datasets>
        <dataset name="daily_sales_data" frequency="${coord:days(1)}" initial-instance="2026-01-20T01:00Z" timezone="UTC">
            <uri-template>hdfs://namenode/data/sales/${YEAR}/${MONTH}/${DAY}</uri-template>
        </dataset>
    </datasets>
    <input-events>
        <data-in name="sales" dataset="daily_sales_data">
            <instance>${coord:current(0)}</instance>
        </data-in>
    </input-events>
    <action>
        <workflow>
            <app-path>${workflowAppPath}</app-path>
        </workflow>
    </action>
</coordinator-app>
```

Este Coordinator instrui o Oozie a executar um workflow (`<app-path>`) todos os dias (`frequency="${coord:days(1)}"`) às 05:00 UTC. Crucialmente, a seção `<input-events>` e `<data-in>` especifica que o workflow só será iniciado se os dados de entrada, definidos pelo template de URI `hdfs://.../${YEAR}/${MONTH}/${DAY}`, estiverem presentes no HDFS para aquele dia específico.

Oozie é a cola que une todo o ecossistema Hadoop, transformando um conjunto de ferramentas individuais em um pipeline de dados coeso, automatizado e robusto. Nos próximos capítulos, mudaremos nosso foco da teoria para a prática, abordando a instalação, configuração e a criação dos nossos laboratórios práticos.


---
# Laboratório 1: Introdução ao HDFS e Comandos Básicos (Duração: 2 horas)

## 1.1 Objetivos

O objetivo deste laboratório é familiarizar você com o Hadoop Distributed File System (HDFS), o sistema de armazenamento primário do Hadoop. Você aprenderá a interagir com o HDFS usando comandos de shell básicos para realizar operações comuns de manipulação de arquivos e diretórios.

## 1.2 Pré-requisitos

-   Um ambiente com Hadoop instalado e configurado (para estes laboratórios, simularemos os comandos em um ambiente Linux que se comporta como um cliente Hadoop).
-   Os datasets gerados na Fase 2, localizados em `/home/ubuntu/hadoop_apostila/datasets/`.

## 1.3 Comandos do HDFS

Os comandos para interagir com o HDFS são muito semelhantes aos comandos do Linux para manipulação de arquivos, mas são prefixados com `hdfs dfs` (ou o mais antigo `hadoop fs`).

| Comando HDFS | Comando Linux Equivalente | Descrição |
| :--- | :--- | :--- |
| `hdfs dfs -ls <path>` | `ls -l <path>` | Lista o conteúdo de um diretório. |
| `hdfs dfs -mkdir -p <path>` | `mkdir -p <path>` | Cria um diretório. A flag `-p` cria os diretórios pais, se necessário. |
| `hdfs dfs -put <local_src> <hdfs_dest>` | `cp <local_src> <dest>` | Copia arquivos do sistema de arquivos local para o HDFS. |
| `hdfs dfs -cat <hdfs_file>` | `cat <file>` | Exibe o conteúdo de um arquivo no HDFS na saída padrão. |
| `hdfs dfs -get <hdfs_src> <local_dest>` | `cp <src> <local_dest>` | Copia arquivos do HDFS para o sistema de arquivos local. |
| `hdfs dfs -rm -r <path>` | `rm -r <path>` | Remove um arquivo ou diretório do HDFS. |
| `hdfs dfs -du -s -h <path>` | `du -s -h <path>` | Mostra o tamanho total dos arquivos em um diretório de forma legível. |

## 1.4 Mão na Massa: Operações no HDFS

Vamos executar uma série de comandos para simular um fluxo de trabalho básico no HDFS. Execute cada um dos seguintes blocos de comando no seu terminal.

### Passo 1: Criar a Estrutura de Diretórios no HDFS

Primeiro, vamos criar uma estrutura de diretórios no HDFS para organizar nossos dados. Criaremos um diretório de usuário e um subdiretório para os dados de entrada.

```bash
# Cria um diretório para o nosso usuário e um subdiretório 'input'
hdfs dfs -mkdir -p /user/aluno/input

# Lista o conteúdo do diretório do usuário para verificar a criação
hdfs dfs -ls /user/aluno
```

### Passo 2: Copiar Dados do Sistema Local para o HDFS

Agora, vamos copiar o arquivo `vendas.csv` que geramos anteriormente do nosso sistema de arquivos local para o diretório `input` que acabamos de criar no HDFS.

```bash
# Copia o arquivo vendas.csv para o HDFS
hdfs dfs -put /home/ubuntu/hadoop_apostila/datasets/vendas.csv /user/aluno/input/

# Verifica se o arquivo foi copiado com sucesso
hdfs dfs -ls /user/aluno/input/
```

### Passo 3: Visualizar o Conteúdo do Arquivo no HDFS

Podemos inspecionar o conteúdo de um arquivo diretamente no HDFS usando o comando `cat`. Vamos visualizar as primeiras linhas do nosso arquivo de vendas para garantir que os dados estão corretos.

```bash
# Exibe o conteúdo completo do arquivo (pode ser muito longo)
# hdfs dfs -cat /user/aluno/input/vendas.csv

# É mais prático usar 'head' combinado com 'cat' para ver apenas o início
hdfs dfs -cat /user/aluno/input/vendas.csv | head -n 5
```

### Passo 4: Verificar o Uso de Espaço

O comando `du` (disk usage) nos ajuda a ver quanto espaço nossos arquivos e diretórios estão ocupando no HDFS.

```bash
# Mostra o tamanho do arquivo vendas.csv de forma legível (e.g., K, M, G)
hdfs dfs -du -h /user/aluno/input/vendas.csv
```

### Passo 5: Copiar Dados do HDFS para o Sistema Local

Também podemos mover dados na direção oposta. Vamos copiar o arquivo do HDFS de volta para o nosso sistema de arquivos local, mas com um nome diferente.

```bash
# Cria um diretório local para a saída
mkdir -p /home/ubuntu/hadoop_apostila/output

# Copia o arquivo do HDFS para o diretório local
hdfs dfs -get /user/aluno/input/vendas.csv /home/ubuntu/hadoop_apostila/output/vendas_backup.csv

# Verifica se o arquivo foi criado no sistema local
ls -lh /home/ubuntu/hadoop_apostila/output/
```

### Passo 6: Limpeza (Opcional)

Para finalizar, podemos remover os arquivos e diretórios que criamos no HDFS.

```bash
# Remove o arquivo
hdfs dfs -rm /user/aluno/input/vendas.csv

# Remove o diretório (só funciona se estiver vazio)
hdfs dfs -rmdir /user/aluno/input

# Para remover um diretório e todo o seu conteúdo de forma recursiva:
hdfs dfs -rm -r /user/aluno

# Verifica se o diretório foi removido
hdfs dfs -ls /user/
```

## 1.5 Conclusão

Parabéns! Você completou seu primeiro laboratório prático com o HDFS. Você aprendeu a navegar no sistema de arquivos distribuído, mover dados entre o ambiente local e o cluster, inspecionar arquivos e gerenciar diretórios. Esses comandos são a base para qualquer trabalho que você fará no ecossistema Hadoop.
# Laboratório 2: Seu Primeiro Job MapReduce com Hadoop Streaming (Duração: 3 horas)

## 2.1 Objetivos

Neste laboratório, você irá criar e executar seu primeiro trabalho MapReduce. Para evitar a complexidade do Java, usaremos o **Hadoop Streaming**, uma ferramenta poderosa que permite usar qualquer executável ou script como seu mapper e/ou reducer. Escreveremos nossos scripts em Python, uma linguagem muito mais acessível para prototipagem rápida.

O objetivo do nosso job será analisar o dataset `web_logs.csv` e **contar a ocorrência de cada método HTTP (GET, POST, etc.)**.

## 2.2 Hadoop Streaming

O Hadoop Streaming funciona com base na entrada e saída padrão (STDIN e STDOUT). O framework faz o seguinte:

-   Para cada tarefa **Map**, ele alimenta os dados de entrada para o STDIN do seu script mapper.
-   O script mapper processa os dados e escreve seus resultados no STDOUT no formato `chave\tvalor`.
-   O framework coleta, ordena e agrupa a saída de todos os mappers.
-   Para cada tarefa **Reduce**, ele alimenta os dados agrupados (uma chave e todos os seus valores associados) para o STDIN do seu script reducer.
-   O script reducer processa os dados e escreve o resultado final no STDOUT.

## 2.3 Mão na Massa: Contando Métodos HTTP

### Passo 1: Preparar os Scripts Mapper e Reducer

Nós já criamos os scripts Python necessários na fase anterior. Vamos revisá-los e garantir que eles sejam executáveis.

-   **Mapper (`mapper_lab2.py`):** Este script lê cada linha do arquivo de log, extrai a coluna correspondente ao método HTTP e emite um par `(método, 1)`.
-   **Reducer (`reducer_lab2.py`):** Este script recebe os pares do mapper, agrupados por método. Para cada método, ele soma todas as contagens (os "1s") para obter o total.

Torne os scripts executáveis:

```bash
chmod +x /home/ubuntu/hadoop_apostila/codigos/mapper_lab2.py
chmod +x /home/ubuntu/hadoop_apostila/codigos/reducer_lab2.py
```

### Passo 2: Testar os Scripts Localmente

Antes de executar um job no cluster, é **essencial** testá-lo localmente para garantir que a lógica está correta. Podemos simular o fluxo do Hadoop Streaming usando pipes (`|`) do Linux.

```bash
# Simula o fluxo completo: cat -> mapper -> sort -> reducer
cat /home/ubuntu/hadoop_apostila/datasets/web_logs.csv | \
/home/ubuntu/hadoop_apostila/codigos/mapper_lab2.py | \
sort -k1,1 | \
/home/ubuntu/hadoop_apostila/codigos/reducer_lab2.py
```

-   `cat`: Envia o conteúdo do arquivo para o STDIN do mapper.
-   `sort -k1,1`: Simula a fase de Shuffle and Sort do Hadoop, garantindo que a entrada para o reducer esteja ordenada pela chave.
-   O resultado final deve ser uma lista de métodos HTTP e suas contagens totais.

### Passo 3: Preparar os Dados no HDFS

Assim como no Laboratório 1, precisamos primeiro colocar nossos dados de entrada no HDFS.

```bash
# Criar um diretório de entrada no HDFS (se ainda não existir)
hdfs dfs -mkdir -p /user/aluno/lab2/input

# Copiar o arquivo de logs para o HDFS
hdfs dfs -put /home/ubuntu/hadoop_apostila/datasets/web_logs.csv /user/aluno/lab2/input/

# Verificar se o arquivo está no HDFS
hdfs dfs -ls /user/aluno/lab2/input/
```

### Passo 4: Executar o Job Hadoop Streaming

Agora estamos prontos para executar o job no cluster Hadoop. O comando `hadoop jar` é usado para invocar o utilitário de streaming.

```bash
hadoop jar /path/to/hadoop-streaming.jar \
    -files /home/ubuntu/hadoop_apostila/codigos/mapper_lab2.py,/home/ubuntu/hadoop_apostila/codigos/reducer_lab2.py \
    -mapper mapper_lab2.py \
    -reducer reducer_lab2.py \
    -input /user/aluno/lab2/input/web_logs.csv \
    -output /user/aluno/lab2/output
```

Vamos analisar este comando:

-   `hadoop jar ...`: Executa o JAR do Hadoop Streaming (o caminho pode variar dependendo da sua instalação).
-   `-files ...`: Informa ao Hadoop para distribuir os scripts do mapper e reducer para todos os nós do cluster, para que eles estejam disponíveis para as tarefas.
-   `-mapper mapper_lab2.py`: Especifica o script a ser usado como mapper.
-   `-reducer reducer_lab2.py`: Especifica o script a ser usado como reducer.
-   `-input /user/aluno/lab2/input/...`: O caminho do arquivo de entrada no HDFS.
-   `-output /user/aluno/lab2/output`: O diretório de saída no HDFS. **Importante:** Este diretório não deve existir antes da execução do job, pois o Hadoop o criará.

### Passo 5: Verificar os Resultados

Após a conclusão do job (o que pode levar alguns minutos), os resultados estarão no diretório de saída que especificamos no HDFS.

```bash
# Listar os arquivos no diretório de saída
hdfs dfs -ls /user/aluno/lab2/output

# Você verá um arquivo como _SUCCESS e um ou mais arquivos part-r-XXXXX
# O arquivo _SUCCESS indica que o job foi concluído com êxito.
# Os arquivos part-r-XXXXX contêm a saída do(s) seu(s) reducer(s).

# Visualizar o resultado
hdfs dfs -cat /user/aluno/lab2/output/part-r-00000
```

A saída deve ser a contagem final de cada método HTTP, semelhante ao que você viu no teste local.

## 2.4 Conclusão

Parabéns! Você executou com sucesso seu primeiro job MapReduce. Você aprendeu o fluxo de trabalho completo: escrever e testar localmente a lógica do mapper e do reducer, mover os dados para o HDFS, executar o job de streaming no cluster e verificar os resultados. Este é um padrão fundamental para o processamento de dados em lote no Hadoop.

No próximo laboratório, vamos explorar o Apache Hive para realizar uma análise mais complexa sobre nossos dados de vendas, mas usando uma abordagem baseada em SQL.
# Laboratório 3: Análise de Dados com Apache Hive (Duração: 4 horas)

## 3.1 Objetivos

Neste laboratório, você aprenderá a usar o Apache Hive para estruturar, consultar e analisar os dados de vendas e clientes que geramos. Você verá como o Hive permite realizar análises complexas usando uma linguagem semelhante ao SQL (HiveQL), sem a necessidade de escrever código MapReduce. Os objetivos específicos são:

1.  Criar tabelas no Hive para impor um esquema sobre nossos arquivos CSV.
2.  Carregar dados do HDFS para as tabelas do Hive.
3.  Executar consultas de agregação e junção (join) para extrair insights.
4.  Aprender sobre o conceito de tabelas particionadas para otimização de consultas.

## 3.2 Mão na Massa: Análise de Vendas com Hive

Para este laboratório, vamos assumir que você está em um ambiente com acesso à linha de comando do Hive (Hive CLI ou Beeline).

### Passo 1: Preparar os Dados no HDFS

Primeiro, vamos garantir que nossos datasets `vendas.csv` e `clientes.csv` estejam no HDFS. Vamos criar um diretório dedicado para os dados do Hive.

```bash
# Criar diretórios no HDFS para os dados das tabelas
hdfs dfs -mkdir -p /user/hive/warehouse/vendas_raw
hdfs dfs -mkdir -p /user/hive/warehouse/clientes_raw

# Copiar os arquivos CSV para os novos diretórios
hdfs dfs -put /home/ubuntu/hadoop_apostila/datasets/vendas.csv /user/hive/warehouse/vendas_raw/
hdfs dfs -put /home/ubuntu/hadoop_apostila/datasets/clientes.csv /user/hive/warehouse/clientes_raw/

# Verificar se os arquivos foram copiados
hdfs dfs -ls /user/hive/warehouse/vendas_raw/
hdfs dfs -ls /user/hive/warehouse/clientes_raw/
```

### Passo 2: Criar Tabelas Gerenciadas no Hive

Agora, vamos iniciar o shell do Hive e criar nossas primeiras tabelas. Estas serão **tabelas gerenciadas**, o que significa que o Hive será responsável por gerenciar os dados e o diretório no HDFS.

Inicie o Hive CLI (o comando pode ser `hive` ou `beeline`):

```sql
-- Criar a tabela para os dados de vendas
CREATE TABLE vendas (
    data_venda STRING,
    produto STRING,
    quantidade INT,
    preco_unitario DOUBLE,
    cidade STRING,
    vendedor STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
-- Ignorar a primeira linha (cabeçalho) do CSV
TBLPROPERTIES ("skip.header.line.count"="1");

-- Criar a tabela para os dados de clientes
CREATE TABLE clientes (
    id_cliente STRING,
    nome STRING,
    email STRING,
    telefone STRING,
    cidade STRING,
    data_cadastro STRING,
    limite_credito DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");
```

### Passo 3: Carregar Dados nas Tabelas

Com as tabelas criadas, vamos carregar os dados dos nossos diretórios `_raw` para as tabelas gerenciadas. O comando `LOAD DATA` move os arquivos para o diretório da tabela gerenciada pelo Hive.

```sql
-- Carregar dados na tabela de vendas
LOAD DATA INPATH 
    '/user/hive/warehouse/vendas_raw/vendas.csv' 
INTO TABLE vendas;

-- Carregar dados na tabela de clientes
LOAD DATA INPATH 
    '/user/hive/warehouse/clientes_raw/clientes.csv' 
INTO TABLE clientes;

-- Verificar se os dados foram carregados (contar as linhas)
SELECT COUNT(*) FROM vendas; -- Deve retornar 10000
SELECT COUNT(*) FROM clientes; -- Deve retornar 5000
```

### Passo 4: Executar Consultas de Análise

Agora que nossos dados estão estruturados no Hive, podemos começar a fazer perguntas interessantes.

**Consulta 1: Qual o total de vendas por produto?**

```sql
SELECT
    produto,
    SUM(quantidade * preco_unitario) AS faturamento_total
FROM vendas
GROUP BY produto
ORDER BY faturamento_total DESC;
```

**Consulta 2: Quais os 5 vendedores que mais venderam?**

```sql
SELECT
    vendedor,
    COUNT(*) AS numero_de_vendas,
    SUM(quantidade * preco_unitario) AS faturamento_total
FROM vendas
GROUP BY vendedor
ORDER BY faturamento_total DESC
LIMIT 5;
```

**Consulta 3: Juntando Vendas e Clientes**

Vamos fazer uma consulta mais complexa. Suponha que a tabela de vendas tivesse um `id_cliente` em vez de `cidade`. Poderíamos juntar as tabelas para descobrir o limite de crédito dos clientes que compraram um determinado produto. *(Para este exercício, vamos juntar por `cidade` apenas para demonstrar a sintaxe do JOIN)*.

```sql
-- Qual o limite de crédito médio dos clientes nas cidades onde vendemos Notebooks?
SELECT
    v.cidade,
    AVG(c.limite_credito) AS media_limite_credito
FROM vendas v
JOIN clientes c ON (v.cidade = c.cidade)
WHERE v.produto = 'Notebook'
GROUP BY v.cidade;
```

### Passo 5: Otimização com Particionamento

Nossas consultas na tabela `vendas` estão lendo a tabela inteira todas as vezes. Se as consultas frequentemente filtrarem por cidade, podemos otimizar isso criando uma tabela particionada.

```sql
-- Criar uma nova tabela de vendas, desta vez particionada por cidade
CREATE TABLE vendas_particionada (
    data_venda STRING,
    produto STRING,
    quantidade INT,
    preco_unitario DOUBLE,
    vendedor STRING
)
PARTITIONED BY (cidade STRING)
STORED AS ORC; -- Usando um formato colunar otimizado (ORC)

-- Para carregar dados em uma tabela particionada, precisamos habilitar o particionamento dinâmico
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Inserir dados da tabela antiga na nova, o Hive criará as partições automaticamente
INSERT OVERWRITE TABLE vendas_particionada PARTITION(cidade)
SELECT 
    data_venda, 
    produto, 
    quantidade, 
    preco_unitario, 
    vendedor, 
    cidade -- A coluna de partição deve ser a última no SELECT
FROM vendas;

-- Verificar as partições criadas
SHOW PARTITIONS vendas_particionada;
```

Agora, se executarmos uma consulta com filtro por cidade na tabela `vendas_particionada`, o Hive irá ler apenas os dados do subdiretório correspondente àquela cidade, tornando a consulta muito mais rápida.

```sql
-- Esta consulta será muito mais rápida que a equivalente na tabela não particionada
SELECT
    produto,
    SUM(quantidade * preco_unitario) AS faturamento_total
FROM vendas_particionada
WHERE cidade = 'São Paulo'
GROUP BY produto
ORDER BY faturamento_total DESC;
```

## 3.3 Conclusão

Neste laboratório, você viu o poder do Apache Hive para transformar análises de Big Data. Você aprendeu a criar esquemas sobre dados brutos, carregar dados e executar consultas complexas usando uma sintaxe familiar de SQL. Mais importante, você aprendeu sobre particionamento, uma técnica fundamental para otimizar o desempenho de consultas em grandes volumes de dados. O Hive é uma ferramenta essencial no cinto de utilidades de qualquer profissional de dados que trabalhe com o ecossistema Hadoop.
# Laboratório 4: Análise de Fluxo de Dados com Apache Pig (Duração: 3 horas)

## 4.1 Objetivos

Neste laboratório, você irá explorar o Apache Pig e sua linguagem, o Pig Latin, para realizar uma análise de fluxo de dados. Enquanto o Hive é excelente para consultas declarativas no estilo SQL, o Pig brilha em cenários de ETL (Extração, Transformação e Carga) e em análises que requerem uma série de passos procedurais. Nosso objetivo será analisar o dataset `web_logs.csv` para **identificar os 5 endereços de IP que mais geraram tráfego (em bytes)**.

## 4.2 O Ambiente Pig

O Pig pode ser executado de duas maneiras principais:

1.  **Modo Grunt:** Um shell interativo onde você pode digitar comandos Pig Latin um por um. É ótimo para exploração e depuração.
2.  **Modo Script:** Onde você escreve um script `.pig` completo e o submete para execução. Este é o modo usado para automatizar pipelines.

Neste laboratório, focaremos no modo script, que é como os pipelines de dados são construídos no mundo real.

## 4.3 Mão na Massa: Analisando Logs com Pig

### Passo 1: Revisar o Script Pig

Já preparamos o script `lab4_analise_logs.pig`. Vamos analisar sua lógica passo a passo, pois ela demonstra perfeitamente o paradigma de fluxo de dados do Pig:

-   **`LOAD`:** O primeiro passo carrega os dados brutos do arquivo CSV no HDFS. Definimos um esquema na leitura, nomeando cada coluna.
-   **`FILTER`:** Como nosso CSV tem um cabeçalho, usamos o `FILTER` para remover a primeira linha, criando uma nova relação de dados limpos.
-   **`GROUP`:** Agrupamos todos os registros pelo endereço de IP. Este é o passo que prepara os dados para a agregação.
-   **`FOREACH ... GENERATE`:** Iteramos sobre cada grupo (cada IP) e, para cada um, usamos a função `SUM()` para calcular o total de bytes. Este passo transforma os dados agrupados em um novo conjunto de dados contendo o IP e o tráfego total.
-   **`ORDER`:** Ordenamos o resultado anterior em ordem decrescente para que os maiores consumidores de tráfego apareçam primeiro.
-   **`LIMIT`:** Selecionamos apenas os 5 primeiros resultados da relação ordenada.
-   **`DUMP`:** Um comando para exibir o resultado final na tela. Em um pipeline de produção, usaríamos `STORE` para salvar o resultado em um novo diretório no HDFS.

### Passo 2: Garantir que os Dados Estão no HDFS

Nosso script Pig espera encontrar o arquivo `web_logs.csv` no HDFS. Vamos garantir que ele esteja no local correto, que preparamos no Laboratório 2.

```bash
# Listar o arquivo para confirmar sua existência
hdfs dfs -ls /user/aluno/lab2/input/web_logs.csv
```

Se o arquivo não estiver lá, copie-o novamente do seu sistema de arquivos local:

```bash
# (Opcional) Copiar o arquivo para o HDFS se necessário
hdfs dfs -mkdir -p /user/aluno/lab2/input
hdfs dfs -put /home/ubuntu/hadoop_apostila/datasets/web_logs.csv /user/aluno/lab2/input/
```

### Passo 3: Executar o Script Pig

Para executar o script, usamos o comando `pig`. O Pig irá analisar o script, convertê-lo em um ou mais jobs MapReduce e submetê-los ao cluster YARN.

```bash
# Executa o script Pig em modo local (para teste rápido, usando o sistema de arquivos local)
# pig -x local /home/ubuntu/hadoop_apostila/codigos/lab4_analise_logs.pig

# Executa o script Pig no cluster Hadoop (modo MapReduce)
pig -x mapreduce /home/ubuntu/hadoop_apostila/codigos/lab4_analise_logs.pig
```

-   `-x mapreduce`: Especifica que o motor de execução é o MapReduce no cluster Hadoop.

A execução pode levar alguns minutos, pois o Pig está traduzindo o script em jobs MapReduce, que são então agendados e executados pelo YARN. Você verá o progresso do job na linha de comando.

### Passo 4: Analisar o Resultado

Quando o script terminar, o comando `DUMP` irá imprimir o resultado diretamente no seu terminal. A saída será uma lista de tuplas, cada uma contendo um endereço de IP e o total de bytes transferidos por ele, ordenados do maior para o menor.

Exemplo de saída:

```
(157.3.142.132, 8547291)
(55.139.137.39, 8512345)
(123.45.67.89, 8499876)
(98.76.54.32, 8488765)
(201.202.203.204, 8477654)
```

Se você tivesse usado o comando `STORE` no final do seu script, os resultados seriam salvos em um arquivo `part-r-00000` dentro do diretório `/user/aluno/lab4/output` no HDFS, que você poderia então inspecionar com `hdfs dfs -cat`.

## 4.4 Conclusão

Neste laboratório, você aprendeu a usar o Apache Pig para construir um pipeline de análise de dados. Você viu como a linguagem Pig Latin permite expressar transformações de dados complexas de forma procedural e passo a passo, o que a torna ideal para tarefas de ETL e preparação de dados. Você também entendeu a diferença fundamental na abordagem entre o Pig (procedural) e o Hive (declarativo).

Com o conhecimento de HDFS, MapReduce, Hive e Pig, você agora tem uma base sólida sobre as principais ferramentas de processamento em lote do ecossistema Hadoop.
# Laboratório 5: Ingestão de Dados com Sqoop e Flume (Duração: 4 horas)

## 5.1 Objetivos

Este laboratório final foca na camada de ingestão de dados do ecossistema Hadoop. Você aprenderá a usar duas ferramentas essenciais para trazer dados para o seu data lake:

1.  **Apache Sqoop:** Para importar dados em massa de um banco de dados relacional.
2.  **Apache Flume:** Para capturar dados de streaming (eventos) em tempo real.

Ao final deste laboratório, você entenderá as diferenças práticas entre essas ferramentas e saberá quando usar cada uma.

## 5.2 Parte 1: Importação de Dados Estruturados com Sqoop

Nesta parte, vamos simular a importação da nossa tabela de `clientes` de um banco de dados relacional (vamos usar o SQLite como um substituto simples para um RDBMS como MySQL ou Oracle) para o HDFS.

### Passo 1: Preparar o Banco de Dados de Origem

Primeiro, precisamos criar um banco de dados SQLite e popular uma tabela `clientes` a partir do nosso arquivo `clientes.csv`. Isso simulará o nosso RDBMS de produção.

```bash
# Instalar o SQLite (se não estiver instalado)
sudo apt-get update && sudo apt-get install -y sqlite3

# Criar o banco de dados e a tabela, e importar os dados do CSV
sqlite3 /home/ubuntu/hadoop_apostila/datasets/clientes.db <<EOF
CREATE TABLE clientes (
    id_cliente TEXT PRIMARY KEY,
    nome TEXT,
    email TEXT,
    telefone TEXT,
    cidade TEXT,
    data_cadastro TEXT,
    limite_credito REAL
);
.mode csv
.import /home/ubuntu/hadoop_apostila/datasets/clientes.csv clientes

-- Remover o cabeçalho que foi importado junto
DELETE FROM clientes WHERE id_cliente = 'id_cliente';

-- Verificar se os dados foram importados
.headers on
SELECT COUNT(*) FROM clientes;
SELECT * FROM clientes LIMIT 5;
.quit
EOF
```

### Passo 2: Executar a Importação com Sqoop

Agora, vamos usar o Sqoop para importar os dados da tabela `clientes` do nosso banco de dados SQLite para o HDFS. O Sqoop irá gerar um job MapReduce para realizar a transferência em paralelo.

*Nota: A conexão com SQLite via Sqoop requer um driver JDBC específico. Para este laboratório, o comando abaixo é uma representação do que seria executado em um ambiente real com um banco de dados como o MySQL.*

```bash
# Comando de importação do Sqoop (exemplo para MySQL)
# Em um ambiente real, você substituiria os detalhes da conexão.
sqoop import \
    --connect jdbc:mysql://localhost/testdb \
    --username root \
    --password-file file:///path/to/password/file \
    --table clientes \
    --target-dir /user/aluno/lab5/sqoop_import/clientes \
    --m 1 -- --schema testdb

# Comando SIMULADO para nosso ambiente com SQLite
# O Sqoop se conectaria ao DB, veria o esquema e executaria mappers.
# Vamos simular o resultado copiando os dados para o HDFS.
echo "Simulando a execução do Sqoop..."
hdfs dfs -mkdir -p /user/aluno/lab5/sqoop_import/clientes
hdfs dfs -put /home/ubuntu/hadoop_apostila/datasets/clientes.csv /user/aluno/lab5/sqoop_import/clientes/
```

### Passo 3: Verificar os Dados no HDFS

Após a conclusão do job Sqoop, os dados da tabela `clientes` estarão no HDFS como arquivos de texto (ou outro formato, se especificado).

```bash
# Verificar os arquivos importados no HDFS
hdfs dfs -ls /user/aluno/lab5/sqoop_import/clientes

# Visualizar o conteúdo de um dos arquivos de saída
hdfs dfs -cat /user/aluno/lab5/sqoop_import/clientes/part-m-00000 | head -n 5
```

## 5.3 Parte 2: Ingestão de Streaming com Flume

Nesta parte, vamos configurar um agente Flume para monitorar um arquivo de log e enviar os novos eventos para o HDFS em tempo real. Isso simula a coleta de logs de um servidor web.

### Passo 1: Configurar o Agente Flume

Vamos criar um arquivo de configuração para o nosso agente Flume. Este agente terá uma fonte (`Source`) que monitora um arquivo, um canal em memória (`Channel`) e um destino (`Sink`) que escreve no HDFS.

Crie o arquivo `/home/ubuntu/hadoop_apostila/codigos/flume_lab.conf`:

```properties
# Nomear os componentes do agente
agent.sources = src1
agent.channels = ch1
agent.sinks = sink1

# Configurar a Source: Spooling Directory
# Monitora um diretório e processa novos arquivos colocados nele.
agent.sources.src1.type = spooldir
agent.sources.src1.spoolDir = /home/ubuntu/hadoop_apostila/datasets/spool
agent.sources.src1.channels = ch1
agent.sources.src1.fileHeader = true

# Configurar o Channel: Memory Channel
agent.channels.ch1.type = memory
agent.channels.ch1.capacity = 1000

# Configurar o Sink: HDFS Sink
agent.sinks.sink1.type = hdfs
agent.sinks.sink1.hdfs.path = hdfs:///user/aluno/lab5/flume_import/logs/
agent.sinks.sink1.hdfs.filePrefix = weblog
agent.sinks.sink1.hdfs.fileSuffix = .log
agent.sinks.sink1.hdfs.rollCount = 10000 -- Inicia um novo arquivo a cada 10000 eventos
agent.sinks.sink1.hdfs.rollSize = 0
agent.sinks.sink1.hdfs.rollInterval = 300 -- Ou a cada 5 minutos
agent.sinks.sink1.hdfs.fileType = DataStream
agent.sinks.sink1.channel = ch1
```

### Passo 2: Iniciar o Agente Flume

Agora, vamos iniciar o agente Flume em um terminal. Ele ficará em execução, esperando por novos arquivos no diretório de spool.

Primeiro, crie o diretório de spool:
`mkdir -p /home/ubuntu/hadoop_apostila/datasets/spool`

Em um terminal, execute (este comando ficará ativo):

```bash
# Inicia o agente Flume
flume-ng agent --conf /home/ubuntu/hadoop_apostila/conf --conf-file /home/ubuntu/hadoop_apostila/codigos/flume_lab.conf --name agent -Dflume.root.logger=INFO,console
```

### Passo 3: Simular a Geração de Logs

Em **outro terminal**, vamos simular um servidor web gerando logs. Faremos isso copiando nosso arquivo `web_logs.csv` para o diretório de spool. O Flume detectará o novo arquivo, o processará e o moverá.

```bash
# Copia o arquivo de log para o diretório que o Flume está monitorando
cp /home/ubuntu/hadoop_apostila/datasets/web_logs.csv /home/ubuntu/hadoop_apostila/datasets/spool/log1.csv

# Aguarde alguns segundos. O Flume irá processar o arquivo e renomeá-lo com o sufixo .COMPLETED
ls -l /home/ubuntu/hadoop_apostila/datasets/spool/
```

### Passo 4: Verificar os Dados no HDFS

Finalmente, vamos verificar se o Flume escreveu com sucesso os dados de log no HDFS.

```bash
# Listar os arquivos criados pelo Flume no HDFS
hdfs dfs -ls /user/aluno/lab5/flume_import/logs/

# Visualizar o conteúdo de um dos arquivos de log
hdfs dfs -cat /user/aluno/lab5/flume_import/logs/weblog-....log | head -n 10
```

Você verá que os dados do arquivo de log agora estão armazenados no HDFS, prontos para serem analisados por jobs MapReduce, Hive ou Pig.

## 5.4 Conclusão

Neste laboratório, você colocou em prática as duas principais formas de ingestão de dados no Hadoop. Você usou o **Sqoop** para uma transferência em massa de dados estruturados de um RDBMS, um caso de uso típico para cargas de dados iniciais ou diárias. Em seguida, você usou o **Flume** para capturar dados de eventos em tempo real, um cenário comum para logs e dados de sensores. Compreender as forças de cada ferramenta é fundamental para projetar e construir pipelines de dados robustos e eficientes no ecossistema Hadoop.
