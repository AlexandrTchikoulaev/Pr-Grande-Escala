# 3. Implementação do Sistema de Análise

## 3.1 Apresentação Geral

O TrendMart é um sistema de análise de dados em grande escala orientado ao suporte à decisão no domínio do retalho de e-commerce. O seu propósito central é simular, ingerir, transformar e analisar dados provenientes de uma plataforma de comércio eletrónico em funcionamento contínuo, transformando eventos brutos dispersos numa infraestrutura analítica unificada capaz de responder a perguntas de negócio que nenhuma das fontes isoladas conseguiria.

O sistema implementa quatro capacidades principais. A primeira é a **ingestão em tempo quase-real**: eventos de comportamento de utilizadores, transações de compra e avaliações de clientes são captados continuamente por três mecanismos de ingestão paralelos e persistidos numa camada de armazenamento de objetos. A segunda é a **preparação e qualidade de dados**: os dados brutos são transformados em representações limpas, tipadas e semanticamente enriquecidas através de jobs Spark com garantias ACID, numa arquitectura progressiva por camadas. A terceira é a **análise descritiva e exploratória**: vistas analíticas SQL pré-computadas sobre um modelo dimensional em estrela são expostas a um dashboard interativo e a ferramentas externas de BI. A quarta é a **análise preditiva**: modelos de machine learning treinados sobre os dados históricos produzem previsões de procura por categoria, disponibilizados como tabelas consumíveis pelo dashboard.

Estas capacidades são implementadas por um pipeline de dados contínuo, organizado em camadas progressivas de transformação e executado por cinco equipas com responsabilidades funcionais delimitadas: **data_engineering**, **analytical_engineering**, **machine_learning**, **infrastructure** e **data_analytics**. O fluxo de dados e a organização em equipas são detalhados nas secções seguintes.

### 3.1.1 Arquitectura Global

A arquitectura organiza-se em camadas funcionais com responsabilidades bem delimitadas e interfaces explícitas entre si, conforme representado na figura seguinte.

```
[Simulador E-commerce]
        │
        ├─► Kafka (clickstream_events)
        ├─► PostgreSQL (simulated_orders)
        └─► MinIO (raw-reviews/)
              │
[Ingestão Contínua]
              │
        ├─► consumer.py        → bronze/clickstream/
        ├─► cdc_consumer.py    → bronze/orders/
        └─► file_watcher.py    → bronze/reviews/
              │
[Transformação Silver (Airflow + Spark)]
              │
        ├─► silver_clickstream → lake.silver.clickstream
        ├─► silver_orders      → lake.silver.orders
        └─► silver_reviews     → lake.silver.reviews
              │
[Transformação Gold (Airflow + Spark)]
              │
        ├─► gold_dimensions  (dim_date, dim_category, dim_geography)
        ├─► gold_sales       → lake.gold.fact_sales
        ├─► gold_clickstream → lake.gold.fact_clickstream
        ├─► gold_reviews     → lake.gold.fact_reviews
        └─► init_views       (6 vistas Trino)
              │
[Machine Learning (Airflow + Spark MLlib)]
              │
        └─► demand_forecast  → lake.gold.ml_demand_forecast
              │
[Dashboard (Dash)]
              └─► 6 abas interativas (refresh a cada 5 min)
```

O fluxo de dados tem origem num simulador de e-commerce que alimenta três fontes heterogéneas em simultâneo: eventos de clickstream publicados em Kafka, ordens de compra persistidas em PostgreSQL e avaliações depositadas em MinIO. Três consumers de ingestão correm em paralelo e de forma contínua, cada um adaptado ao protocolo da sua fonte, persistindo os dados em bruto na camada Bronze do Data Lake em ficheiros Parquet. Esta camada é imutável — preserva os dados no formato original, sem qualquer transformação de conteúdo, e serve como fonte de verdade histórica do sistema.

A transformação Silver, orquestrada pelo Airflow com cadência horária, lê os ficheiros Bronze acumulados e produz três tabelas Apache Iceberg limpas, tipadas e deduplicadas. Sobre estas tabelas, a transformação Gold — também orquestrada pelo Airflow na mesma DAG horária — constrói um modelo dimensional em estrela com três dimensões e três tabelas de factos, expondo os resultados através de seis vistas Trino directamente consumíveis pelo dashboard.

Em paralelo, um pipeline de Machine Learning independente, com cadência diária às 03:00 UTC, treina modelos de previsão de procura sobre as tabelas Gold e publica os resultados como uma nova tabela consumível. O dashboard consulta as vistas Trino e as previsões ML de cinco em cinco minutos, apresentando os resultados em seis abas interativas sem necessidade de transformações no cliente.

A divisão deste fluxo em equipas com responsabilidades e contratos explícitos é descrita na secção 3.1.2.

### 3.1.2 Equipas, Responsabilidades e Contratos de Interface

O fluxo de dados descrito na secção anterior é implementado por cinco equipas com âmbitos funcionais estritamente delimitados. Cada equipa tem responsabilidades bem definidas, materializadas num contrato de entrada — o que consome, de quem, com que schema e com que frequência — e num contrato de saída — o que entrega, a quem, com que garantias. Uma equipa não acede a artefactos que não constam do seu contrato de entrada, nem produz artefactos fora do seu contrato de saída: é este mecanismo que torna as partes do sistema operacionalmente independentes e substituíveis sem acoplamento entre áreas.

O simulador (data_sources) não é uma equipa de dados no sentido técnico — é o componente que substitui a plataforma de e-commerce real, gerando continuamente os dados que alimentam o pipeline. A sua caracterização detalhada encontra-se na secção 3.2.

A figura seguinte apresenta o mesmo fluxo de dados da secção 3.1.1, agora mapeado sobre as equipas responsáveis por cada fase:

```
── data_sources ──────────────────────────────────────────────────────
  Simulador E-commerce
        ├─► Kafka (clickstream_events)
        ├─► PostgreSQL (simulated_orders)
        └─► MinIO (raw-reviews/)
              ↓
── data_engineering ──────────────────────────────────────────────────
  Ingestão Contínua
        ├─► consumer.py        → bronze/clickstream/
        ├─► cdc_consumer.py    → bronze/orders/
        └─► file_watcher.py    → bronze/reviews/
              ↓
  Transformação Silver (Airflow + Spark)
        ├─► silver_clickstream → lake.silver.clickstream
        ├─► silver_orders      → lake.silver.orders
        └─► silver_reviews     → lake.silver.reviews
              ↓
── analytical_engineering ────────────────────────────────────────────
  Transformação Gold (Airflow + Spark)
        ├─► gold_dimensions  (dim_date, dim_category, dim_geography)
        ├─► gold_sales       → lake.gold.fact_sales
        ├─► gold_clickstream → lake.gold.fact_clickstream
        ├─► gold_reviews     → lake.gold.fact_reviews
        └─► init_views       (6 vistas Trino)
              ↓
── machine_learning ──────────────────────────────────────────────────
  DAG Diária (Airflow + Spark MLlib)
        └─► demand_forecast  → lake.gold.ml_demand_forecast
              ↓
── data_analytics ────────────────────────────────────────────────────
  Dashboard (Dash)
        └─► 6 abas interativas (refresh a cada 5 min)

── infrastructure (transversal) ──────────────────────────────────────
  Docker Compose (12 serviços) + Airflow (trendmart_gold_pipeline
  + trendmart_ml_pipeline) — suporta todas as equipas acima
```

#### Equipa de Engenharia de Dados

Responsável pela ingestão contínua das três fontes do simulador e pela transformação dos dados brutos em tabelas analíticas limpas, tipadas e deduplicadas. A implementação detalhada encontra-se na secção 3.3.

**Contrato de entrada**

- tópico Kafka `clickstream_events`
- tópico Kafka `debezium.public.simulated_orders`
- bucket MinIO `raw-reviews/`

**Contrato de saída** — três tabelas Apache Iceberg no catálogo lake, actualizadas com cadência horária

| Tabela | Garantias |
|--------|-----------|
| lake.silver.clickstream | Schema fixo, tipagem forte, sem duplicados por event_id, com os 11 tipos de evento do funil de conversão |
| lake.silver.orders | Validado, enriquecido com region, sem duplicados por order_id (MERGE INTO ACID) |
| lake.silver.reviews | Rating extraído de texto livre, sem duplicados por review_id (MERGE INTO ACID) |

#### Equipa de Engenharia Analítica

Responsável pela construção do modelo dimensional Gold e pela exposição dos dados ao dashboard através de vistas SQL Trino. A implementação detalhada encontra-se na secção 3.4.

**Contrato de entrada**

- lake.silver.clickstream
- lake.silver.orders
- lake.silver.reviews

**Contrato de saída**

| Artefacto | Tipo | Padrão de escrita |
|-----------|------|-------------------|
| dim_date, dim_category, dim_geography | Dimensões Iceberg | createOrReplace a cada run |
| fact_sales, fact_clickstream, fact_reviews | Factos Iceberg | Append incremental por janela horária |
| vw_executive, vw_sales_performance, vw_funnel, vw_reviews, vw_trends, vw_category_trends | Vistas Trino | Criadas/substituídas após cada run Gold |

#### Equipa de Machine Learning

Responsável pela camada preditiva — treina modelos sobre os dados históricos Gold e publica previsões de procura por categoria. A implementação detalhada encontra-se no capítulo 5.

**Contrato de entrada**

- lake.gold.fact_sales
- lake.gold.dim_date
- lake.gold.dim_category

**Contrato de saída**

| Tabela | Conteúdo | Padrão de escrita |
|--------|----------|-------------------|
| lake.gold.ml_demand_forecast | Previsões D+1 a D+7 por categoria, com predicted_orders, model_rmse, model_mae e scored_at | createOrReplace a cada execução diária |

#### Equipa de Engenharia de Infraestrutura

Responsável pela plataforma de serviços e pela orquestração dos pipelines. Não produz nem transforma dados — garante que todas as outras equipas conseguem operar. A implementação detalhada encontra-se na secção 3.5.

**Contrato de entrada** — requisitos de SLA e schedules comunicados pelas equipas consumidoras

**Contrato de saída** — plataforma operacional com 12 serviços containerizados (Docker Compose) e dois pipelines Airflow agendados: `trendmart_gold_pipeline` (horário) e `trendmart_ml_pipeline` (diário, 03:00 UTC)

#### Equipa de Analistas de Dados

Responsável por transformar os dados analíticos em respostas a perguntas de negócio concretas, através de um dashboard interativo. A implementação detalhada encontra-se no capítulo 4.

**Contrato de entrada** — os seis perfis de utilizador e os 22 requisitos funcionais definidos no capítulo 2

**Contrato de saída** — insights e conclusões accionáveis para suporte à decisão

| Perfil | Perguntas de negócio respondidas |
|--------|----------------------------------|
| Gestor Executivo | Como evoluiu a receita? Quantos clientes únicos compraram? Qual o rating médio? |
| Analista de Vendas | Quais as categorias e regiões com maior receita? Qual o impacto do frete? |
| Analista de Marketing | Onde abandona o funil? Que dispositivos dominam? Que categorias são mais exploradas? |
| Gestor de Customer Experience | Que categorias têm pior satisfação? Como evolui o sentimento ao longo do tempo? |
| Analista de Tendências | Que categorias crescem? Há anomalias de procura? |
| Data Scientist | Qual a procura prevista por categoria nos próximos 7 dias? |


## 3.2 Fontes de Dados

Na ausência de uma plataforma de e-commerce em produção, o TrendMart simula o funcionamento de uma loja digital através de um simulador contínuo desenvolvido especificamente para o projeto. O simulador utiliza como referência os dados reais do dataset Olist Brazilian E-Commerce — produtos, clientes, preços e avaliações —, garantindo coerência entre as três fontes geradas: os produtos que surgem no comportamento de navegação são os mesmos que originam compras e avaliações.

A escolha das três fontes foi orientada por três objetivos deliberados. O primeiro foi a **heterogeneidade estrutural**: as fontes diferem intencionalmente em formato e natureza — eventos semi-estruturados, registos transacionais estruturados e texto livre —, tornando impossível uma solução puramente relacional e justificando a adopção de uma arquitectura Data Lake. O segundo foi a **escala**: o clickstream produz dados de forma contínua a alta frequência, representando os padrões de data at scale presentes em plataformas reais. O terceiro foi o **realismo**: as três fontes replicam os tipos de dados dominantes numa plataforma de e-commerce real — comportamento de navegação, transações de compra e feedback de clientes.

As três fontes produzidas pelo simulador são resumidas na tabela seguinte:

| Fonte | Formato | Periodicidade | Estrutura | Destino |
|-------|---------|---------------|-----------|---------|
| Clickstream de Navegação | JSON | Contínuo (~0,5 sess/s) | Semi-estruturado | Kafka (`clickstream_events`) |
| Ordens de Compra | JSON | Contínuo (por compra) | Estruturado | PostgreSQL (`simulated_orders`) |
| Avaliações de Clientes | Texto livre (.txt) | Contínuo (por compra) | Não estruturado | MinIO (`raw-reviews/`) |

### 3.2.1 Clickstreams de Navegação

O clickstream de navegação regista a sequência de interações realizadas pelos utilizadores durante uma sessão na plataforma. Cada sessão é gerada pelo módulo `session.py` através da função `simulate_session()`, que modela o percurso completo do utilizador num funil probabilístico e produz a lista ordenada de eventos da sessão. Os eventos são publicados no tópico Kafka `clickstream_events` usando o `session_id` como chave de partição, garantindo que todos os eventos de uma sessão ficam na mesma partição e a sua ordem temporal é preservada.

#### Estrutura dos Eventos

Cada evento segue uma estrutura base comum com os seguintes campos:

| Campo | Descrição |
|-------|-----------|
| event_id | Identificador único do evento |
| session_id | Identificador da sessão |
| user_id | Identificador do utilizador |
| event_type | Tipo de interação no funil |
| timestamp | Momento de geração em UTC |
| device | Tipo de dispositivo (mobile, desktop, tablet) |
| properties | Objeto JSON com atributos específicos do tipo de evento |

O campo `properties` é semi-estruturado — a sua composição varia consoante o `event_type`:

| event_type | Propriedades |
|------------|--------------|
| search | query, category |
| product_view | product_id, category, price |
| add_to_cart | product_id, price |
| cart_view | product_id, total |
| checkout_start | product_id, total |
| session_end | reason |

#### Funil de Conversão

O comportamento do utilizador é modelado por um funil de conversão probabilístico implementado em `session.py`. Cada sessão inicia com `session_start` e progride através de etapas com probabilidades condicionais fixas:

```
session_start
    │
    ├─► search (60%)  ─────────────────────────────────────────────────┐
    └─► category_browse (40%) ─────────────────────────────────────────┤
                                                                        │
    ┌───────────────────────────────────────────────────────────────────┘
    │
    ├─► [bounce: session_end(reason=bounce)] ← 30% não avança
    │
    └─► product_view (70%)
            │
            ├─► product_review_read (40%, opcional)
            │
            ├─► [session_end(reason=no_cart)] ← 65% não adiciona ao carrinho
            │
            └─► add_to_cart (35%)
                    │
                    ├─► remove_from_cart + add_to_cart (15%, re-add)
                    │
                    └─► cart_view
                            │
                            ├─► cart_abandon + session_end(reason=cart_abandon) ← 45%
                            │
                            └─► checkout_start (55%)
                                    │
                                    ├─► session_end(reason=checkout_abandon) ← 20%
                                    │
                                    └─► order_placed (80%) → session_end(reason=completed)
```

A taxa de conversão global implícita é de aproximadamente 10,8% (0,70 × 0,35 × 0,55 × 0,80), alinhada com benchmarks reais de e-commerce. Cada evento tem um significado analítico distinto:

- **`session_start`**: início da sessão; contém o contexto geográfico do utilizador (estado e cidade)
- **`search`**: navegação por intenção explícita — o termo de pesquisa é o nome da categoria do produto
- **`category_browse`**: navegação por descoberta — o utilizador explora uma categoria sem intenção de pesquisa definida
- **`product_view`**: visualização de um produto específico; inclui preço e número de fotografias
- **`product_review_read`**: consulta de avaliações de outros compradores antes de decidir (40% dos casos)
- **`add_to_cart`**: adição ao carrinho — sinal forte de intenção de compra
- **`remove_from_cart`**: remoção do carrinho antes de nova adição — simula hesitação na decisão
- **`cart_view`**: visualização do carrinho com o total calculado (produto + frete)
- **`cart_abandon`**: abandono do carrinho sem prosseguir para checkout
- **`checkout_start`**: início do processo de checkout — intenção de compra muito forte
- **`session_end`**: encerramento da sessão com campo `reason` (`bounce`, `no_cart`, `cart_abandon`, `checkout_abandon`, `completed`)

O evento `order_placed` não é publicado no Kafka — é encaminhado exclusivamente para o PostgreSQL via `db_writer.py`.

#### Ruído

O módulo `noise.py` introduz perturbações controladas nos eventos: ausência de `session_id` (simulando falhas de rastreamento), valores inválidos no campo `device` (bots e crawlers) e ausência de `event_type` (falhas de instrumentação). Estes eventos são filtrados e normalizados na camada Silver.

### 3.2.2 Ordens de Compra

As ordens de compra são geradas pela função `simulate_session()` em `session.py` sempre que uma sessão culmina em conversão. O registo é escrito diretamente na tabela `simulated_orders` do PostgreSQL via `db_writer.py`. Os campos `session_id` e `order_id` são comuns às duas fontes, permitindo correlacionar o comportamento de navegação com a transação resultante.

#### Estrutura do Registo

Cada registo de ordem segue um schema fixo:

| Campo | Descrição |
|-------|-----------|
| order_id | Identificador único da transação |
| session_id | Identificador da sessão de navegação associada |
| customer_id | Identificador do cliente |
| product_id | Identificador do produto adquirido |
| seller_id | Identificador do vendedor |
| category | Categoria do produto |
| price | Valor do produto no momento da compra |
| freight_value | Custo de envio |
| state | Estado do cliente |
| purchase_timestamp | Momento da transação |

#### Ruído

Cerca de 5% dos registos têm o código de estado mal formatado — variações de capitalização ou caracteres adicionais —, simulando inconsistências típicas em sistemas de entrada de dados. A normalização é feita na camada Silver.

### 3.2.3 Avaliações de Clientes

As avaliações são geradas de forma desacoplada pelo módulo `review_writer.py`, que associa uma review a aproximadamente 40% das ordens concluídas, com um atraso controlado entre 30 e 300 segundos — simulando o tempo real entre compra e receção do produto. Cada avaliação é persistida como ficheiro `.txt` no bucket MinIO `raw-reviews/`, com o nome `{review_id}_{order_id}.txt`.

#### Estrutura do Ficheiro

O conteúdo é texto livre em português, sem campos delimitados — os únicos metadados estruturados estão codificados no nome do ficheiro. O texto termina sempre com a classificação do produto numa de duas formas:

- *"Dou X estrelas em 5."*
- *"Classifico este produto com X/5."*

Este padrão permite a extração do rating por expressão regular na camada Silver. O `raw_content` completo é preservado no Bronze para processamento posterior.


## 3.3 Área de Preparação de Dados

A área de preparação de dados é a componente do pipeline responsável por receber dados heterogéneos das três fontes do simulador e os transformar em representações analíticas limpas, tipadas e semanticamente enriquecidas, prontas a alimentar o modelo dimensional Gold.

A implementação desta área é da responsabilidade da equipa data_engineering. O seu contrato delimita com precisão o que esta área exige: receber dados brutos das três fontes heterogéneas como entrada e entregar, como saída, tabelas Silver com schema fixo, tipagem forte e deduplicação garantida.

### 3.3.1 Justificação da Abordagem

A área de preparação de dados do projeto é implementada segundo uma arquitectura Data Lakehouse com padrão Medallion (Bronze / Silver / Gold), sobre armazenamento de objetos MinIO. O Data Lakehouse combina as características de um Data Lake — capacidade de receber qualquer dado no seu formato original, sem schema obrigatório na entrada — com as características de um Data Warehouse — garantias ACID, schema enforced e capacidade de consulta SQL analítica. No TrendMart esta divisão manifesta-se de forma explícita por camadas: o Bronze é a componente "lake", onde os dados chegam em bruto e sem transformação; o Silver e o Gold são a componente "house", onde os dados adquirem schema fixo, tipagem forte, garantias transacionais via Apache Iceberg e capacidade de consulta SQL via Trino. As características específicas de cada camada são detalhadas nas secções 3.3.2 e 3.3.3.

A adopção desta arquitectura decorre da natureza heterogénea das três fontes de dados — eventos JSON num tópico Kafka, eventos CDC com envelope Debezium, e ficheiros de texto com formato não estruturado — que torna impossível a inserção directa num schema relacional fixo sem processamento prévio. Um sistema puramente relacional exigiria schema definido na entrada, incompatível com a variabilidade do envelope CDC e com a estrutura não estruturada dos ficheiros de review. A arquitectura Lakehouse resolve este problema ao aceitar qualquer dado na camada Bronze sem imposição de schema, diferindo essa imposição para a camada Silver, e só então promovendo os dados para o modelo analítico Gold.

A separação progressiva em camadas traz três vantagens concretas:

1. **Rastreabilidade** — em caso de erro de transformação, os dados originais estão sempre preservados no Bronze, permitindo reprocessamento sem re-ingestão da fonte.
2. **Idempotência** — o Spark Structured Streaming mantém um checkpoint que regista os ficheiros Bronze já consumidos, tornando o reprocessamento seguro mesmo após falhas parciais sem risco de duplicação.
3. **Separação de responsabilidades** — os consumers de ingestão escrevem apenas no Bronze, sem conhecimento das regras de qualidade da camada Silver, mantendo os dois processos independentes e substituíveis.

### 3.3.2 Camada Bronze

#### Objetivo

A camada Bronze é o ponto de entrada de todos os dados no Data Lakehouse e constitui a sua componente "lake" — a fonte de verdade histórica imutável do sistema. O seu objectivo é único e deliberadamente restrito: receber dados das três fontes no formato mais próximo possível do original e preservá-los sem qualquer transformação de conteúdo. A única modificação introduzida pelos consumers é a adição de dois campos de metadados de ingestão — ingested_at (timestamp de receção) e source (mecanismo de ingestão: kafka, debezium ou minio) — que permitem rastrear a proveniência de cada registo sem alterar o dado em si.

Esta imutabilidade tem uma consequência arquitectural central: em caso de erro ou alteração das regras de qualidade na camada Silver, é sempre possível reprocessar os dados originais a partir do Bronze sem necessidade de re-ingestão da fonte. A camada Bronze desacopla assim o ritmo de chegada dos dados do ritmo do seu processamento, tornando os dois processos independentes e substituíveis sem perda de informação.

#### Fonte 1 — Clickstream de Navegação

**Origem**

Os eventos de clickstream têm origem no módulo simulator/producer.py, que publica cada evento no tópico Kafka clickstream_events em formato JSON. O Kafka actua como buffer durável entre o simulador e o consumer de ingestão, garantindo que nenhum evento se perde mesmo que o consumer esteja temporariamente indisponível.

**Extração e Transporte**

Para extrair e transportar os eventos o módulo ingestion/consumer.py subscreve o tópico Kafka e consome as mensagens em modo contínuo. Cada mensagem JSON é desserializada e o campo properties — que é um objecto JSON aninhado com estrutura variável por event_type — é serializado de volta para string antes de ser escrito, preservando o dado bruto sem impor schema fixo na ingestão. Esta decisão é intencional: forçar o parsing do campo properties no Bronze introduziria um schema rígido incompatível com a variabilidade da fonte; a desserialização fica reservada para a camada Silver.

**Armazenamento**

Os eventos são acumulados em memória e escritos em ficheiros Parquet no sub-bucket bronze/clickstream/, com particionamento hierárquico em quatro níveis: year, month, day, hour.

#### Fonte 2 — Ordens de Compra (CDC)

**Origem**

As ordens de compra estão escritas na tabela simulated_orders do PostgreSQL.

**Extracção**

O Debezium monitoriza continuamente o Write-Ahead Log (WAL) do PostgreSQL e captura cada mutação da tabela — INSERT, UPDATE ou snapshot inicial — publicando-a no tópico Kafka debezium.public.simulated_orders encapsulada num envelope JSON com os campos before, after, op e ts_ms. Este mecanismo de Change Data Capture permite capturar a totalidade do ciclo de vida de cada registo sem qualquer alteração ao schema ou ao código do simulador.

**Transporte**

O módulo ingestion/cdc_consumer.py subscreve o tópico Debezium e desembrulha o envelope CDC para extrair o campo after, que contém o estado completo da linha após a mutação. São processadas as operações c (INSERT), u (UPDATE) e r (snapshot inicial); os eventos d (DELETE) são descartados porque o modelo de dados do simulador não produz eliminações de ordens. Os campos cdc_operation e cdc_ts_ms são preservados no registo Parquet para que a camada Silver consiga rastrear a origem e a ordem temporal de cada evento. A inclusão dos UPDATEs no Bronze é intencional: o simulador executa uma operação UPDATE sem alteração de dados (noop_update) para simular o comportamento de at-least-once delivery do Debezium, gerando um segundo evento com o mesmo order_id; se esses UPDATEs fossem descartados no Bronze, o mecanismo de deduplicação MERGE INTO da camada Silver não teria oportunidade de actuar.

**Armazenamento**

Os registos são escritos em ficheiros Parquet no sub-bucket bronze/orders/, com o mesmo esquema de particionamento de quatro níveis do clickstream: year, month, day, hour.

#### Fonte 3 — Avaliações de Clientes (File Polling)

**Origem**

As avaliações são escritas pelo simulador como ficheiros de texto no bucket MinIO raw-reviews/. Cada ficheiro tem o nome `{review_id}_{order_id}.txt` — os únicos metadados estruturados associados à avaliação estão codificados no nome do ficheiro, não no seu conteúdo.

**Extracção e transporte**

O módulo ingestion/file_watcher.py inspeciona o bucket raw-reviews/ a cada 30 segundos via API S3 compatível com MinIO. Para cada ficheiro .txt ainda não processado — identificado por comparação com o conjunto de object_keys registados em .watcher_state.json — o review_id e o order_id são extraídos do nome do ficheiro por partição no carácter _, e o conteúdo completo é lido e incluído como campo raw_content no registo Parquet, preservando o texto livre intacto para extracção de informação na camada Silver. Após a escrita do batch, o object_key de cada ficheiro processado é adicionado ao ficheiro de estado, garantindo que o mesmo ficheiro nunca é ingerido duas vezes mesmo após reinicialização do processo.

**Armazenamento**

As avaliações são escritas em ficheiros Parquet no sub-bucket bronze/reviews/, com particionamento em três níveis: year, month, day — um nível a menos do que as outras duas fontes, dado o volume mais reduzido e a ausência de granularidade horária no mecanismo de polling.

#### Métodos Comuns

**Particionamento hierárquico no padrão Hive**

O particionamento é o mecanismo pelo qual os ficheiros Parquet são organizados em subpastas hierárquicas dentro de cada sub-bucket, seguindo a convenção campo=valor reconhecida nativamente pelo Spark e pelo PyArrow. O seu objectivo é a eliminação de partições na leitura: quando o job Silver processa apenas os dados de uma determinada hora, o Spark filtra a query pela partição correspondente e ignora completamente todas as outras pastas — evitando a leitura e desserialização de ficheiros irrelevantes, o que reduz significativamente o volume de I/O e o tempo de execução em proporção directa com a selectividade temporal do filtro.

O clickstream e as ordens utilizam quatro níveis de partição (year, month, day, hour) porque são gerados em modo contínuo com volume horário expressivo, tornando a granularidade horária relevante para a eliminação de partições. As avaliações utilizam três níveis (year, month, day) porque o seu volume mais reduzido e a cadência de polling de 30 segundos não justificam subdivisão horária. Cada ficheiro dentro da partição é nomeado `batch_<unix_timestamp>.parquet`:

```
bronze/clickstream/year=2026/month=05/day=25/hour=08/batch_1748174400.parquet
bronze/orders/year=2026/month=05/day=25/hour=08/batch_1748174400.parquet
bronze/reviews/year=2026/month=05/day=25/batch_1748174400.parquet
```

**Micro-batching**

O micro-batching é a estratégia pela qual os consumers acumulam registos em memória antes de os escrever num único ficheiro Parquet, em vez de escreverem um ficheiro por registo. O seu objectivo é equilibrar dois requisitos conflituantes: baixa latência de disponibilização dos dados no Bronze e eficiência de leitura pelo Spark. Ficheiros demasiado pequenos — um por mensagem Kafka, por exemplo — degradam o desempenho de leitura do Spark por excesso de overhead de metadata do sistema de ficheiros; ficheiros demasiado grandes aumentam o tempo que os dados aguardam em memória antes de ficarem disponíveis no Bronze.

Os consumers Kafka (consumer.py) e CDC (cdc_consumer.py) implementam este equilíbrio com dois critérios de flush alternativos: o buffer é escrito quando atinge 500 registos ou quando decorrem 30 segundos desde o último flush, o que ocorrer primeiro. Este duplo critério garante que em períodos de tráfego elevado os ficheiros têm dimensão razoável, e que em períodos de tráfego reduzido os dados não ficam retidos em memória por mais de 30 segundos. O file watcher (file_watcher.py) não mantém buffer contínuo entre ciclos — escreve um único ficheiro Parquet com todos os ficheiros .txt novos encontrados em cada ciclo de polling de 30 segundos.

### 3.3.3 Camada Silver

#### Objectivo

A camada Silver é a componente "house" do Data Lakehouse — o ponto onde os dados deixam de ser armazenamento bruto e passam a ser uma base analítica estruturada. O seu objectivo é transformar os ficheiros Parquet do Bronze em tabelas Apache Iceberg limpas, tipadas, semanticamente enriquecidas e com garantias ACID, prontas a ser consumidas pela equipa analytical_engineering para a construção do modelo dimensional Gold. É nesta camada que se concentra toda a lógica de qualidade de dados do sistema: a camada Bronze apenas preserva; a camada Silver garante.

Cada um dos três jobs Spark de transformação Silver lê os ficheiros Parquet do Bronze correspondente, aplica regras de validação e enriquecimento específicas ao tipo de dado, e escreve o resultado numa tabela Iceberg registada no catálogo lake através do Hive Metastore.

#### Job 1 — silver_clickstream.py

**Entrada**

Ficheiros Parquet de bronze/clickstream/, lidos com schema explícito para evitar inferência automática.

**Transformações e regras de qualidade**

O campo properties — preservado como string JSON no Bronze — é desserializado para struct com schema definido, extraindo os atributos específicos de cada event_type (product_id, category, price, etc.). Os eventos com session_id nulo ou vazio são descartados, pois sem identificador de sessão não é possível reconstruir qualquer sequência de navegação. Os eventos sem event_type válido ou sem event_ts são igualmente descartados. Os valores do campo device são normalizados para o vocabulário controlado {mobile, desktop, tablet}: valores não reconhecidos — como "bot" ou "crawler", injectados pelo simulador como ruído — são normalizados para "unknown". Os eventos recebidos correspondem implicitamente aos 11 tipos do funil de conversão — o Kafka producer exclui o evento `order_placed` antes da publicação, pelo que o Silver nunca recebe eventos fora do vocabulário esperado.

**Saída**

Tabela Iceberg lake.silver.clickstream com schema fixo e tipagem forte, em modo append.

#### Job 2 — silver_orders.py

**Entrada**

Ficheiros Parquet de bronze/orders/, que contêm eventos CDC do Debezium com os campos operacionais da tabela simulated_orders e os metadados cdc_operation e cdc_ts_ms.

**Transformações e regras de qualidade**

Os campos price e freight_value são convertidos de string para DOUBLE — o Debezium serializa colunas NUMERIC do PostgreSQL como string binária no Parquet — e os registos com price ≤ 0 ou freight_value < 0 são descartados como inválidos. O campo state é validado contra o conjunto dos 27 códigos de estado brasileiros: estados não reconhecidos (como "sao paulo") têm state definido como NULL e region como "Desconhecido", sem descartar a ordem. O campo total_value é calculado como round(price + freight_value, 2). Cada ordem é enriquecida com o campo region, derivado do mapeamento dos 27 estados para as cinco macrorregiões brasileiras.

**Saída**

Tabela Iceberg lake.silver.orders, com deduplicação garantida por dois níveis (detalhados na secção Mecanismos Comuns).

#### Job 3 — silver_reviews.py

**Entrada**

Ficheiros Parquet de bronze/reviews/, que contêm o campo raw_content com o texto integral de cada avaliação e os campos review_id e order_id já extraídos do nome do ficheiro pelo file_watcher na camada de ingestão.

**Transformações e regras de qualidade**

O Silver valida que os campos review_id e order_id não são nulos e têm conteúdo; registos com identificadores em falta são descartados. O rating é extraído do texto livre por duas expressões regulares correspondentes às duas formas produzidas pelo simulador — *"Dou X estrelas em 5"* e *"Classifico este produto com X/5"* —; registos para os quais nenhum padrão produz um valor válido no intervalo [1, 5] são descartados. O text_length é calculado como contagem de palavras por split em espaços. O raw_content completo é preservado como campo message para consumo posterior.

**Saída**

Tabela Iceberg lake.silver.reviews, com deduplicação garantida por MERGE INTO (detalhado na secção Mecanismos Comuns).

#### Mecanismos Comuns

**Checkpoint do Spark Structured Streaming**

Os três jobs utilizam `trigger(availableNow=True)`, que instrui o Spark a processar todos os ficheiros Bronze ainda não consumidos e a terminar — em vez de correr indefinidamente como um stream contínuo. O estado de progresso é mantido num checkpoint persistido em MinIO (s3a://silver/_checkpoints/{clickstream,orders,reviews}): o Spark regista neste checkpoint os ficheiros Bronze já processados em runs anteriores e, no início de cada novo run, lê apenas os ficheiros ainda não registados. Este mecanismo garante idempotência: se um job falhar a meio e for relançado pela DAG, não reprocessa dados já escritos na Silver.

**Deduplicação com MERGE INTO Iceberg**

Os jobs de ordens e reviews enfrentam um problema adicional de deduplicação: o Bronze pode conter múltiplos eventos para o mesmo order_id (devido ao at-least-once delivery do Debezium) ou para o mesmo review_id (devido às submissões duplicadas injectadas pelo simulador). A deduplicação é feita em dois níveis. Dentro de cada micro-batch processado pelo Spark, uma window function (ROW_NUMBER OVER PARTITION BY order_id/review_id ORDER BY cdc_ts_ms DESC) retém apenas o evento mais recente por chave. Entre runs, o MERGE INTO Iceberg com WHEN NOT MATCHED THEN INSERT * garante que um order_id ou review_id já existente na Silver nunca é duplicado por runs posteriores — independentemente do número de vezes que o mesmo evento apareça no Bronze. O job de clickstream não necessita deste mecanismo porque os eventos de clickstream são naturalmente únicos por event_id e são escritos em modo append directo.

O código dos três jobs é da responsabilidade da equipa data_engineering; a execução é agendada e despoletada pela DAG trendmart_gold_pipeline da equipa infrastructure (infrastructure/dags/dag_trendmart.py), com cadência horária definida como requisito de SLA pela equipa analytical_engineering.


## 3.4 Engenharia Analítica

A equipa analytical_engineering transforma os dados operacionais limpos da camada Silver num modelo analítico pronto a consumir — o modelo dimensional Gold — e expõe-o ao dashboard através de seis vistas SQL Trino. A equipa consome exclusivamente as tabelas Silver entregues pela data_engineering e produz o conjunto de artefactos que constituem a interface entre o pipeline de dados e a camada de análise: dimensões, tabelas de factos e vistas que encapsulam toda a lógica de agregação e cálculo de métricas derivadas, tornando o dashboard um consumidor passivo de dados já prontos para visualização.

Esta secção descreve o modelo dimensional (3.4.1), a sua implementação física em Apache Iceberg (3.4.2) e as vistas Trino que expõem o modelo (3.4.3). O consumo destas vistas pelo dashboard e o seu valor analítico por perfil de utilizador são detalhados no capítulo 4.

### 3.4.1 Modelo Dimensional

O sistema de dados analítico do TrendMart implementa um modelo dimensional em esquema estrela na camada Gold do Data Lakehouse, armazenado em tabelas Apache Iceberg sobre MinIO e acessível via Trino. A escolha de um modelo dimensional — em detrimento de uma modelação relacional normalizada (3NF) — decorre dos padrões de acesso analítico dominantes: queries que atravessam múltiplas dimensões (por exemplo, "receita por categoria, por mês, por região"), contagens e agregações sobre grandes volumes de factos, e filtragens sobre atributos descritivos das dimensões. O esquema estrela minimiza o número de joins necessários por query analítica e torna as estruturas de dados imediatamente compreensíveis para analistas sem conhecimento técnico do pipeline. O processo de conceção seguiu os quatro passos do método de Kimball, com as escolhas de design ancoradas nos requisitos funcionais do capítulo 2.

#### Passo 1 — Identificação dos Processos de Negócio

Foram identificados três processos de negócio distintos mas complementares, cada um correspondendo a uma tabela de factos:

- **Vendas e Encomendas**: o ciclo de vida de uma encomenda desde a compra até à sua conclusão, com métricas financeiras — suporta os perfis Gestor Executivo (RF1.1–RF1.4), Analista de Vendas (RF2.1–RF2.4) e Analista de Tendências (RF5.1–RF5.4);
- **Comportamento de Navegação**: os eventos que os utilizadores geram na plataforma, que permitem analisar o funil de conversão digital — suporta o perfil Analista de Marketing (RF3.1–RF3.4);
- **Avaliações de Clientes**: o feedback qualitativo pós-compra, com classificação de sentimento — suporta o perfil Gestor de Customer Experience (RF4.1–RF4.4).

#### Passo 2 — Definição da Granularidade

Para fact_sales, a granularidade é ao nível da ordem individual: cada linha representa uma transação de compra. Esta granularidade permite agregar métricas em qualquer nível superior (categoria, dia, região) sem perda de informação.

Para fact_clickstream, a granularidade é ao nível do evento individual: cada linha representa um clique, visualização, ou acção no funil. Esta granularidade permite reconstruir sessões completas e calcular taxas de conversão em qualquer ponto do funil.

Para fact_reviews, a granularidade é ao nível da avaliação individual: cada linha representa um ficheiro de review processado, com o rating, sentimento, categoria e região associados.

#### Passo 3 — Identificação das Dimensões

A seleção das dimensões partiu da análise dos eixos de análise exigidos pelos requisitos funcionais do capítulo 2. Todos os perfis necessitam de análise temporal; os perfis de natureza ERP (Analista de Vendas, Analista de Tendências) e o Gestor de Customer Experience necessitam de análise por categoria; o Analista de Vendas e o Gestor de Customer Experience necessitam de análise geográfica. Isto determina exactamente três dimensões, necessárias e suficientes:

- **dim_date**: responde ao *quando*. Criada como calendário completo de 2020 a 2030 (3 652 datas), com atributos derivados: year, month, quarter, week, day_of_week, is_weekend. É partilhada pelas três tabelas de factos e pelas tabelas ML.
- **dim_category**: responde ao *o quê* ao nível de categoria. Contém os nomes de categoria derivados das ordens e do clickstream, com `category_id` gerado por hash determinístico do nome. Permite análise por categoria em qualquer tabela de factos.
- **dim_geography**: responde ao *onde*. Mapeia os 27 estados brasileiros para as cinco macrorregiões (Norte, Nordeste, Centro-Oeste, Sudeste, Sul), com `geo_id` gerado por hash do código de estado.

Não foram criadas dimensões ao nível de produto ou de cliente porque nenhum requisito funcional do capítulo 2 exige análise a esse nível de detalhe — toda a análise de produto é feita ao nível de categoria (RF2.1, RF4.3, RF5.4) e toda a análise de cliente é expressa em contagens de clientes únicos por dia (RF1.1, RF2.2), não em atributos de clientes individuais.

#### Passo 4 — Identificação dos Factos

fact_sales contém métricas aditivas (price, freight_value, total_value), as chaves de negócio order_id, customer_id e session_id, e chaves estrangeiras para as três dimensões (date_id, category_id, geo_id). O customer_id é preservado como atributo degenerado para permitir a contagem de clientes únicos (RF1.1, RF2.2) sem necessidade de uma dimensão dim_customer.

fact_clickstream contém event_type e device como factos degenerados (vocabulários controlados com número fixo de valores), session_id e user_id como chaves de negócio, e chaves estrangeiras para dim_date (date_id) e dim_category (category_id). Não tem FK para dim_geography porque nenhum requisito do perfil Analista de Marketing (RF3.1–RF3.4) exige análise geográfica do comportamento de navegação.

fact_reviews contém rating (semi-aditivo — o indicador relevante é a média, não a soma), sentiment como facto degenerado (positive, neutral, negative, classificado em gold_reviews.py), e chaves estrangeiras para as três dimensões (date_id, category_id, geo_id). O geo_id é resolvido em gold_reviews.py por join com silver.orders para obter o estado do cliente da ordem associada, seguido de lookup em dim_geography — garantindo que a geografia fica inscrita no próprio facto e não depende de um join posterior através de fact_sales, o que causaria NULLs em reviews cujas ordens ainda não tivessem chegado ao Gold no momento da execução da DAG.

A tabela ml_demand_forecast é uma tabela Gold adicional que não faz parte do modelo dimensional principal mas partilha a convenção de nomenclatura e é acessível via Trino. É gerada pela DAG independente trendmart_ml_pipeline e consumida directamente pelo dashboard sem passar por vistas.

### 3.4.2 Implementação Física do Schema

Os jobs Spark da equipa analytical_engineering são invocados pela DAG trendmart_gold_pipeline com cadência horária. Existem dois padrões de escrita distintos consoante o tipo de tabela.

#### Dimensões — createOrReplace a cada run

Os três jobs de dimensões (gold_dimensions.py) recriam integralmente dim_date, dim_category e dim_geography a cada execução via writeTo().createOrReplace(). Esta escolha é deliberada: as dimensões têm um conjunto de valores pequeno e estável, e recalculá-las completamente garante que qualquer nova categoria ou novo estado presente no Silver fica reflectido imediatamente, sem lógica de merge ou detecção de diferenças.

#### Tabelas de factos

Os três jobs de factos (gold_sales.py, gold_clickstream.py, gold_reviews.py) usam um modelo de carga incremental baseado na janela de execução do Airflow. O DAG passa a cada job os parâmetros data_interval_start e data_interval_end — os timestamps exatos da hora que está a ser processada —, e o job filtra a tabela Silver pelo campo ingested_at dentro dessa janela:

```
ingested_at >= data_interval_start  AND  ingested_at < data_interval_end
```

O campo ingested_at é atribuído por-registo no momento em que cada consumer Bronze recebe a mensagem (do Kafka, do CDC ou do MinIO), pelo que reflecte o instante real de ingestão de cada evento individual — não o timestamp do ficheiro Parquet que o contém.

Na primeira execução, quando a tabela Gold ainda não existe, o job usa writeTo().createOrReplace() para a criar com o schema e particionamento corretos. Nas execuções seguintes usa writeTo().append(), adicionando apenas os registos da hora processada. Se não existirem registos na janela — por exemplo num período de tráfego nulo —, o job termina sem escrever, evitando commits vazios.

Este modelo é distinto do mecanismo incremental da camada Silver: os jobs Silver usam Spark Structured Streaming com trigger(availableNow=True) e um checkpoint persistido em MinIO, que rastreia quais ficheiros Bronze já foram lidos pelo path — sem dependência do Airflow. O Gold delega essa responsabilidade ao orquestrador, o que torna cada run Gold determinístico e reproduzível: re-executar a janela das 13:00–14:00 produz sempre o mesmo resultado, independentemente de quando a re-execução ocorre.

#### Tabelas ML — createOrReplace diário

A tabela ml_demand_forecast é substituída na totalidade a cada execução diária da DAG trendmart_ml_pipeline via createOrReplace, porque o modelo é retreinado de raiz com o histórico completo disponível nesse momento.

#### Catálogo partilhado

O catálogo Iceberg é gerido pelo Hive Metastore (protocolo Thrift), partilhado entre o Spark (escrita) e o Trino (leitura). Esta partilha permite ao Trino ver imediatamente as tabelas escritas pelo Spark sem registo manual — é o que torna as tabelas Gold consultáveis pelo dashboard logo após cada execução da DAG.

### 3.4.3 Vistas Trino

As seis vistas são criadas pelo script `analytical_engineering/views/init_views.py`, executado como última task da DAG `trendmart_gold_pipeline` após a conclusão de todos os jobs Gold. Encapsulam joins entre factos e dimensões, agregações e métricas derivadas — expondo os dados no formato exactamente esperado pelo dashboard sem necessidade de lógica de negócio no cliente. Cada vista serve um perfil de utilizador definido no capítulo 2 e satisfaz os requisitos funcionais correspondentes.

**vw_executive** — Perfil: Gestor Executivo (RF1.1–RF1.4)

Agrega os dados diariamente ao nível global — sem desagregação por categoria ou região —, produzindo uma linha por dia com as colunas `total_orders`, `total_customers`, `total_revenue`, `avg_order_value`, `avg_rating`, `positive_reviews` e `negative_reviews`. Responde a RF1.1 (KPIs globais diários), RF1.2 (evolução diária da receita), RF1.3 (volume diário de reviews positivas e negativas) e RF1.4 (rating médio diário).

**vw_sales_performance** — Perfil: Analista de Vendas (RF2.1–RF2.4)

Opera ao nível da combinação (data, categoria, estado, região), expondo `purchase_date`, `category`, `state`, `region`, `orders`, `customers`, `revenue`, `avg_order_value`, `product_revenue` e `freight_revenue`. A granularidade ao nível da data permite ao dashboard agregar em qualquer dimensão superior sem perda de informação. Responde a RF2.1 (receita por categoria e região), RF2.2 (clientes únicos por dia), RF2.3 (impacto do frete) e RF2.4 (volume de ordens por período).

**vw_funnel** — Perfil: Analista de Marketing (RF3.1–RF3.4)

Agrega os eventos de clickstream com granularidade horária, produzindo uma linha por combinação de (hora, data, tipo de evento, dispositivo, categoria) com `event_count`, `sessions` e `users`. O evento `order_placed` não existe no clickstream — é injectado sinteticamente via UNION ALL sobre `fact_sales`, contribuindo com um registo por ordem concluída. Responde a RF3.1 (análise do funil), RF3.2 (preferências de dispositivo), RF3.3 (padrões de navegação por hora) e RF3.4 (categorias mais exploradas).

**vw_reviews** — Perfil: Gestor de Customer Experience (RF4.1–RF4.4)

Agrega as avaliações por combinação de (data, sentimento, categoria, região), expondo `review_count`, `avg_rating` e `avg_text_length`. Os joins de categoria e geografia são feitos directamente sobre os campos `category_id` e `geo_id` de `fact_reviews` — o `geo_id` é resolvido em `gold_reviews.py` por join com `silver.orders`, pelo que a vista não necessita de atravessar `fact_sales` para obter a geografia. Responde a RF4.1 (volume e rating médio por categoria), RF4.2 (evolução do sentimento), RF4.3 (categorias com pior satisfação) e RF4.4 (distribuição geográfica das avaliações).

**vw_trends** — Perfil: Analista de Tendências (RF5.1–RF5.3)

Agrega os dados diariamente ao nível global, calculando a taxa de crescimento week-over-week (WoW) de receita e pedidos, a aceleração da procura (variação da taxa de crescimento entre semanas consecutivas) e uma flag de anomalia para dias com desvio superior a 2σ da média calculada sobre uma janela histórica de 60 dias (`ROWS BETWEEN 59 PRECEDING AND CURRENT ROW`). Janela de consulta: 60 dias.

**vw_category_trends** — Perfil: Analista de Tendências (RF5.4)

Fornece as mesmas métricas de crescimento WoW com desagregação por categoria e semana, com uma janela de 30 dias. Complementa `vw_trends` ao nível de detalhe por categoria exigido por RF5.4.
