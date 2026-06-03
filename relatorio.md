**TrendMart**** — Relatório do Trabalho de Unidade Curricular**

**Resumo**
*[A preencher após a conclusão das restantes secções — **máx**. 250 palavras]*
**Palavras-chave:** e-commerce, data lake, medallion architecture, streaming, machine learning, demand forecasting

**1. Definição do Sistema**
*[A preencher]*

**2. Levantamento e Análise de Requisitos**
O desenvolvimento de um sistema de análise de dados de grande escala exige, antes de qualquer decisão técnica, uma compreensão clara de quem vai usar o sistema e para quê. Sem esta ancoragem, corre-se o risco de construir uma infraestrutura tecnicamente sofisticada mas incapaz de responder às perguntas de negócio que justificam a sua existência. O levantamento de requisitos é, por isso, o primeiro passo estruturante do projeto — o momento em que se traduzem necessidades de negócio em especificações concretas que guiam todas as decisões de arquitetura e implementação subsequentes.

No caso do TrendMart, o sistema serve uma organização de e-commerce com múltiplas áreas funcionais, cada uma com perspetivas, objetivos e ritmos de trabalho distintos. Em empresas reais, estas necessidades analíticas estão tipicamente distribuídas entre dois tipos de sistemas de informação: sistemas de ERP (Enterprise Resource Planning), que gerem dados operacionais e transacionais como vendas, receita e inventário, e sistemas de CRM (Customer Relationship Management), que centralizam dados de comportamento de cliente, interações, feedback e satisfação. O TrendMart replica analiticamente os dados que estes dois tipos de sistemas gerariam, consolidando-os numa camada analítica unificada.

A interface que concretiza este serviço é o dashboard desenvolvido pela equipa `data_analytics`. Esta equipa é técnica e única — mas o produto que entrega ramifica-se em seis abas, cada uma orientada a um perfil de utilizador distinto. 
Os perfis de natureza ERP — Gestor Executivo, Analista de Vendas e Analista de Tendências — consomem dados transacionais e operacionais: receita, volume de vendas, crescimento e anomalias. 
Os perfis de natureza CRM — Analista de Marketing, Gestor de Customer Experience e Data Scientist — consomem dados comportamentais e relacionais: funil de conversão, sentimento de cliente e previsão de procura. É esta ramificação que serve de estrutura ao levantamento de requisitos deste capítulo: cada aba corresponde a um perfil, cada perfil tem perguntas de negócio próprias, e cada pergunta origina requisitos funcionais concretos que o sistema tem de satisfazer.

Este capítulo organiza-se em três secções: o método de levantamento adotado (2.1), a caracterização dos seis perfis com os respetivos requisitos funcionais e não funcionais (2.2), e a validação de cada requisito contra a implementação real do sistema (2.3).

2.1 Método Adotado
O método adotado foi a análise de cenário orientada a perfis de utilizador. Para cada um dos seis perfis identificados, foram definidas as perguntas de negócio que o sistema deve ser capaz de responder, os dados necessários para o efeito, e as restrições operacionais específicas ao seu contexto de uso. Os requisitos funcionais derivam diretamente dessas perguntas — um requisito está satisfeito se e só se a pergunta que lhe corresponde pode ser respondida com o sistema construído.
Esta abordagem tem três vantagens metodológicas. Primeiro, força a ancoragem dos requisitos em necessidades concretas de negócio em vez de capacidades técnicas abstratas. Segundo, facilita a priorização — funcionalidades partilhadas por múltiplos perfis têm maior prioridade do que as específicas a um único perfil, o que guiou as decisões de arquitetura no sentido de maximizar o valor entregue com os recursos disponíveis. Terceiro, torna a validação dos requisitos sistemática — na secção 2.3, cada requisito é verificado contra a implementação real do sistema, tornando explícita a correspondência entre o que foi especificado e o que foi construído.
Para além dos requisitos funcionais por perfil, foram identificados requisitos não funcionais transversais — relativos a latência, idempotência, rastreabilidade, escalabilidade e reprodutibilidade — que condicionam a arquitetura e não são específicos a nenhum perfil de utilizador.

2.2 Organização dos Requisitos Levantados

2.2.1 Perfil 1 — Gestor Executivo

O Gestor Executivo é responsável por monitorizar a saúde global do negócio e tomar decisões estratégicas com base em indicadores de desempenho de alto nível. O seu interesse não está no detalhe operacional — está na evolução da receita, no volume de encomendas, na satisfação geral dos clientes e no crescimento período a período. Trabalha com horizontes temporais de 90 dias e necessita que os dados estejam atualizados com frequência suficiente para detetar tendências antes que se tornem problemas.

Perguntas de negócio:
Qual foi a receita total, o número de encomendas e o valor médio por encomenda no período?
Quantos clientes únicos realizaram compras?
Como evoluiu a receita diariamente ao longo dos últimos 90 dias?
Qual o rating médio das avaliações e como se distribuem entre positivas e negativas?

Requisitos funcionais derivados:
RF1.1 — KPIs globais diários: receita total, volume de encomendas, clientes únicos e valor médio por encomenda, em série temporal de 90 dias
RF1.2 — Evolução diária da receita em formato de série temporal
RF1.3 — Volume diário de reviews positivas e negativas
RF1.4 — Rating médio diário com referência visual ao limiar neutro

2.2.2 Perfil 2 — Analista de Vendas
O Analista de Vendas é responsável por avaliar o desempenho comercial da plataforma ao nível do detalhe — por categoria de produto, por região geográfica e por evolução temporal. O seu interesse está em perceber quais as categorias que mais contribuem para a receita, como se distribui geograficamente a base de clientes, e como evoluem as vendas ao longo do tempo. Trabalha com horizontes de 90 dias e necessita de vistas desagregadas que permitam identificar oportunidades e áreas de melhoria no portfólio de produtos. Este perfil é de natureza ERP — consome dados transacionais com granularidade por categoria, região e estado.
Perguntas de negócio que o sistema deve responder para este perfil:
Quais são as categorias de produto com maior contribuição para a receita?
Como se distribui a receita por região e por estado?
Como evoluiu a receita ao longo do tempo?
Qual a decomposição da receita entre valor de produto e custos de frete por categoria?
Requisitos funcionais derivados:
RF2.1 — O sistema deve disponibilizar as top 10 categorias por receita total num horizonte de 90 dias.
RF2.2 — O sistema deve disponibilizar a distribuição de receita por macrorregião e por estado.
RF2.3 — O sistema deve disponibilizar a evolução temporal da receita com granularidade diária.
RF2.4 — O sistema deve disponibilizar a decomposição de receita em valor de produto e valor de frete por categoria e por período.

2.2.3 Perfil 3 — Analista de Marketing
O Analista de Marketing é responsável por monitorizar o comportamento de navegação dos utilizadores na plataforma e avaliar a eficácia do funil de conversão. O seu interesse está em perceber em que etapa do funil os utilizadores abandonam, como se distribui o comportamento por tipo de dispositivo, e como evoluem as sessões e utilizadores ativos ao longo do tempo. Trabalha com horizontes de 7 dias e necessita de vistas que permitam identificar pontos de fricção no percurso do utilizador antes da compra. Este perfil é de natureza CRM — consome dados comportamentais gerados pelo clickstream.

Perguntas de negócio que o sistema deve responder para este perfil:
Qual é o volume de eventos em cada etapa do funil de conversão e onde ocorre maior abandono?
Como se distribui o comportamento de navegação por dispositivo — mobile, desktop e tablet?
Quais os eventos mais frequentes ao longo do tempo?
Como evoluiu o número de sessões e utilizadores ativos nos últimos 7 dias?

Requisitos funcionais derivados:
RF3.1 — O sistema deve disponibilizar o funil completo de eventos — desde session_start até order_placed — com contagens por etapa.
RF3.2 — O sistema deve desagregar os eventos por tipo de dispositivo de acesso.
RF3.3 — O sistema deve disponibilizar a evolução temporal dos top 5 eventos mais frequentes com granularidade diária numa janela de 7 dias.
RF3.4 — O sistema deve disponibilizar o número de sessões e utilizadores únicos por dia.

2.2.4 Perfil 4 — Gestor de Customer Experience
O Gestor de Customer Experience é responsável por monitorizar a qualidade da experiência do cliente após a compra, nomeadamente através da análise das avaliações e do sentimento expresso. O seu interesse está em identificar padrões de insatisfação — categorias com ratings baixos, regiões com avaliações negativas recorrentes, deterioração temporal do sentimento — antes que se traduzam em perda de clientes. Trabalha com horizontes de 90 dias e necessita de vistas que permitam detetar precocemente sinais de degradação da experiência. Este perfil é de natureza CRM — consome dados de feedback e satisfação de cliente.

Perguntas de negócio que o sistema deve responder para este perfil:
Como se distribui o sentimento das avaliações entre positivo, neutro e negativo?
Como evoluiu o rating médio ao longo do tempo?
Quais as categorias com maior volume de reviews e qual o seu rating médio?
Como se distribui o sentimento por região geográfica?

Requisitos funcionais derivados:
RF4.1 — O sistema deve disponibilizar a distribuição de reviews por sentimento — positivo, neutro e negativo — com base na classificação do rating.
RF4.2 — O sistema deve disponibilizar a evolução do rating médio diário numa janela de 90 dias.
RF4.3 — O sistema deve disponibilizar as top 10 categorias por volume de reviews com o respetivo rating médio.
RF4.4 — O sistema deve disponibilizar a distribuição de sentimento por macrorregião geográfica.

2.2.5 Perfil 5 — Analista de Tendências
O Analista de Tendências é responsável por identificar padrões de crescimento, aceleração da procura e anomalias no comportamento das vendas. Ao contrário do Analista de Vendas, que olha para o desempenho absoluto, este perfil foca-se na variação relativa — crescimento semana-sobre-semana, aceleração dessa taxa de crescimento, e deteção de situações extraordinárias que se desviam do padrão esperado. É o perfil mais diretamente alinhado com o objetivo central do enunciado: a deteção precoce de tendências de produto. Trabalha com horizontes de 60 dias e necessita de métricas de variação temporal com granularidade semanal. Este perfil é de natureza ERP — consome dados transacionais agregados com métricas derivadas de séries temporais.

Perguntas de negócio que o sistema deve responder para este perfil:
Qual foi o crescimento da receita e do volume de encomendas semana-sobre-semana?
A taxa de crescimento está a acelerar ou a desacelerar?
Existem anomalias no padrão de vendas nos últimos 60 dias?
Quais as categorias com maior e menor crescimento médio nos últimos 30 dias?

Requisitos funcionais derivados:
RF5.1 — O sistema deve calcular e disponibilizar o crescimento semana-sobre-semana da receita e do volume de encomendas.
RF5.2 — O sistema deve calcular e disponibilizar a aceleração da procura — variação da taxa de crescimento entre semanas consecutivas.
RF5.3 — O sistema deve detetar e sinalizar anomalias no padrão de vendas numa janela de 60 dias.
RF5.4 — O sistema deve disponibilizar o crescimento médio por categoria numa janela de 30 dias.

2.2.6 Perfil 6 — Data Scientist
O Data Scientist é responsável por interpretar os resultados dos modelos preditivos treinados pelo pipeline de machine learning e monitorizar a qualidade das previsões ao longo do tempo. O seu interesse está na disponibilidade das previsões de procura por categoria para os próximos sete dias e nas métricas de desempenho do modelo que as suportam. Ao contrário dos restantes perfis, não consome apenas dados de negócio — consome também métricas de avaliação de modelos que lhe permitem aferir a fiabilidade das previsões antes de as utilizar em decisões operacionais. Este perfil é de natureza CRM — as previsões de procura derivam de padrões comportamentais e transacionais dos clientes.
Perguntas de negócio que o sistema deve responder para este perfil:
Qual é a previsão de encomendas para os próximos 7 dias por categoria de produto?
Qual o desempenho preditivo do modelo em termos de RMSE e MAE por categoria?
Requisitos funcionais derivados:
RF6.1 — O sistema deve treinar um modelo de previsão de procura por categoria e persistir as previsões para os sete dias seguintes com as métricas de desempenho associadas.
RF6.2 — O sistema deve disponibilizar as previsões de procura e as métricas do modelo num dashboard interativo, atualizadas a cada execução diária do pipeline ML.

2.2.7 Requisitos Não Funcionais

Os requisitos não funcionais identificados são transversais a todos os perfis e condicionam as decisões de arquitetura do sistema.
RNF1 — Latência analítica: os dados disponibilizados no dashboard devem refletir os eventos gerados pelo simulador com uma latência máxima de uma hora, correspondente à cadência do pipeline batch de transformação Silver → Gold. O dashboard atualiza as suas vistas de cinco em cinco minutos.
RNF2 — Idempotência: qualquer operação de processamento ou carga deve poder ser repetida sem duplicar ou corromper dados. Este requisito aplica-se a todos os jobs Spark de transformação Silver (via operação MERGE INTO Iceberg), ao pipeline Gold (via modo append incremental com watermark) e às tabelas ML (via createOrReplace a cada execução diária).
RNF3 — Rastreabilidade: os dados originais devem ser preservados em formato imutável na camada Bronze do Data Lake, independentemente de qualquer transformação posterior. Em caso de erro na camada Silver ou no pipeline Gold, deve ser possível reprocessar os dados originais sem contactar novamente a fonte.
RNF4 — Reprodutibilidade: o sistema deve ser completamente reproduzível a partir do código-fonte e do dataset Olist público, sem dependência de dados externos não disponíveis. A reprodutibilidade é garantida pelo Docker Compose e pelo dataset Olist carregado na inicialização do simulador.
RNF5 — Escalabilidade horizontal: a arquitetura deve suportar aumento do volume de dados sem alteração estrutural do pipeline. A adoção do Apache Spark como motor de processamento garante que os jobs de transformação escalam horizontalmente com a adição de executores, sem alteração do código.
RNF6 — Orquestração auditável: a execução dos pipelines de dados deve ser gerida por um orquestrador com suporte a dependências entre tasks, retentativas automáticas e registo do estado de execução de cada run. Este requisito fundamenta a escolha do Apache Airflow.

2.3 Análise e Validação Geral dos Requisitos
A tabela seguinte valida cada requisito funcional contra a implementação real do sistema, organizada por perfil de utilizador. Para cada requisito é identificado o artefacto concreto que o satisfaz.

Perfil 1 — Gestor Executivo

| **Requisito** | **Artefacto de Satisfação** |
| --- | --- |
| RF1.1 — KPIs globais diários (receita, encomendas, clientes, valor médio) | vw_executive (total_revenue, total_orders, total_customers, avg_order_value); aba Executivo |
| RF1.2 — Evolução diária da receita | vw_executive (total_revenue por day); gráfico de área na aba Executivo |
| RF1.3 — Volume diário de reviews positivas e negativas | vw_executive (positive_reviews, negative_reviews); gráfico de barras na aba Executivo |
| RF1.4 — Rating médio diário | vw_executive (avg_rating); gráfico de linha na aba Executivo |

Perfil 2 — Analista de Vendas

| **Requisito** | **Artefacto de Satisfação** |
| --- | --- |
| RF2.1 — Top 10 categorias por receita | vw_sales_performance (category_en, revenue); aba Vendas |
| RF2.2 — Distribuição por macrorregião e estado | vw_sales_performance (region, state — via dim_geography); aba Vendas |
| RF2.3 — Evolução temporal da receita com granularidade diária | vw_sales_performance (purchase_date, revenue); aba Vendas |
| RF2.4 — Decomposição receita produto vs. frete | vw_sales_performance (product_revenue, freight_revenue — gold_sales.py) |

Perfil 3 — Analista de Marketing

| **Requisito** | **Artefacto de Satisfação** |
| --- | --- |
| RF3.1 — Funil completo de eventos | vw_funnel (event_type, event_count); aba Funil |
| RF3.2 — Desagregação por dispositivo | vw_funnel (device — normalizado em silver_clickstream.py); aba Funil |
| RF3.3 — Evolução top 5 eventos (7 dias) | vw_funnel (event_date, event_type, event_count — janela 7 dias em data.py); aba Funil |
| RF3.4 — Sessões e utilizadores únicos por dia | vw_funnel (sessions, users por event_date); aba Funil |

Perfil 4 — Gestor de Customer Experience

| **Requisito** | **Artefacto de Satisfação** |
| --- | --- |
| RF4.1 — Distribuição de reviews por sentimento | gold_reviews.py — CASE WHEN rating >= 4 THEN 'positive' WHEN rating = 3 THEN 'neutral' ELSE 'negative'; vw_reviews (sentiment, review_count) |
| RF4.2 — Evolução do rating médio diário (90 dias) | vw_reviews (review_date, avg_rating — janela 90 dias em data.py); aba Reviews |
| RF4.3 — Top 10 categorias por volume de reviews | vw_reviews (category, review_count, avg_rating); aba Reviews |
| RF4.4 — Sentimento por macrorregião | vw_reviews (region, sentiment — via dim_geography); aba Reviews |

Perfil 5 — Analista de Tendências

| **Requisito** | **Artefacto de Satisfação** |
| --- | --- |
| RF5.1 — Crescimento WoW da receita e encomendas | vw_trends (revenue_growth_pct, orders_growth_pct); aba Tendências |
| RF5.2 — Aceleração da procura | vw_trends (revenue_acceleration); gráfico de barras na aba Tendências |
| RF5.3 — Deteção de anomalias (60 dias) | vw_trends (anomaly_flag); marcadores no gráfico WoW na aba Tendências |
| RF5.4 — Crescimento médio por categoria (30 dias) | vw_category_trends (category, revenue_growth_pct); aba Tendências |

Perfil 6 — Data Scientist

| **Requisito** | **Artefacto de Satisfação** |
| --- | --- |
| RF6.1 — Previsão de procura por categoria (7 dias) com RMSE e MAE | demand_forecast.py + lake.gold.ml_demand_forecast (forecast_date D+1 a D+7, predicted_orders, model_rmse, model_mae) |
| RF6.2 — Previsões no dashboard atualizadas diariamente | data.py — query a lake.gold.ml_demand_forecast; aba ML Insights |

Requisitos Não Funcionais

| **Requisito** | **Artefacto de Satisfação** |
| --- | --- |
| RNF1 — Latência analítica (máx. 1h pipeline + 5min dashboard) | DAG dag_trendmart com schedule='0 * * * *' (horária); dashboard com Interval(5min) |
| RNF2 — Idempotência | MERGE INTO Iceberg em Silver; append incremental em Gold; createOrReplace nas tabelas ML |
| RNF3 — Rastreabilidade | Bucket bronze/ imutável; reprocessamento possível sem re-ingestão |
| RNF4 — Reprodutibilidade | Docker Compose com todos os serviços; dataset Olist público carregado na inicialização |
| RNF5 — Escalabilidade horizontal | Apache Spark com Iceberg; adição de executores sem alteração de código |
| RNF6 — Orquestração auditável | Apache Airflow — DAG dag_trendmart (horária) e DAG trendmart_ml_pipeline (diária às 03:00) |

São 28 requisitos no total — 22 funcionais distribuídos pelos seis perfis e 6 não funcionais transversais, todos satisfeitos pela implementação. Durante o processo de levantamento e validação foram identificadas duas anomalias. A primeira diz respeito ao formato dos ficheiros de avaliações no bucket MinIO: a documentação inicial do contrato de saída do sistema Data Sources descrevia um formato com cabeçalho estruturado que não correspondia à implementação real — os ficheiros contêm texto narrativo em português sem campos delimitados, com metadata apenas no nome do ficheiro. A ação corretiva foi a atualização do contrato de saída e da documentação associada. A segunda anomalia refere-se à taxa de conversão global: este valor não é pré-calculado na vista vw_funnel, sendo calculado em tempo de execução pelo dashboard a partir das contagens de session_start e order_placed — uma opção que mantém a vista simples mas delega o cálculo ao cliente analítico.

**3. Implementação do Sistema de Análise**
3.1 Apresentação Geral
O TrendMart é um sistema de análise de dados em grande escala orientado ao suporte à decisão no domínio do retalho de e-commerce. O seu propósito central é simular, ingerir, transformar e analisar dados provenientes de uma plataforma de comércio eletrónico em funcionamento contínuo, transformando eventos brutos dispersos numa infraestrutura analítica unificada capaz de responder a perguntas de negócio que nenhuma das fontes isoladas conseguiria.
Do ponto de vista funcional, o sistema implementa quatro capacidades principais. A primeira é a ingestão em tempo quase-real: eventos de comportamento de utilizadores, transações de compra e avaliações de clientes são captados continuamente por três mecanismos de ingestão paralelos e persistidos numa camada de armazenamento de objetos. A segunda é a preparação e qualidade de dados: os dados brutos são transformados em representações limpas, tipadas e semanticamente enriquecidas através de jobs Spark com garantias ACID, numa arquitectura progressiva por camadas. A terceira é a análise descritiva e exploratória: vistas analíticas SQL pré-computadas sobre um modelo dimensional em estrela são expostas a um dashboard interativo e a ferramentas externas de BI. A quarta é a análise preditiva: modelos de machine learning treinados sobre os dados históricos produzem previsões de procura por categoria e produto, disponibilizados como tabelas consumíveis pelo dashboard.

3.1.1 Arquitectura Global
A arquitectura organiza-se em seis camadas funcionais com responsabilidades bem delimitadas e interfaces explícitas entre si, conforme representado na figura seguinte.
[Simulador E-commerce]
        │
        ├─► Kafka (clickstream_events)
        ├─► PostgreSQL (simulated_orders)
        └─► MinIO (raw-reviews/)
              │
        [Data Engineering — Ingestão Contínua]
              │
        ├─► consumer.py        → bronze/clickstream/
        ├─► cdc_consumer.py    → bronze/orders/
        └─► file_watcher.py    → bronze/reviews/
              │
        [Data Engineering — Transformação Horária (Spark)]
              │
        ├─► silver_clickstream → lake.silver.clickstream
        ├─► silver_orders      → lake.silver.orders
        └─► silver_reviews     → lake.silver.reviews
              │
        [Analytical Engineering — DAG Horária (Airflow + Spark)]
              │
        ├─► gold_dimensions  (dim_date, dim_category, dim_geography)
        ├─► gold_sales       → lake.gold.fact_sales
        ├─► gold_clickstream → lake.gold.fact_clickstream
        ├─► gold_reviews     → lake.gold.fact_reviews
        └─► init_views       (6 vistas Trino)
              │
        [Machine Learning — DAG Diária (Airflow + Spark MLlib)]
              │
        ├─► demand_forecast  → lake.gold.ml_demand_forecast
              │
        [Data Analytics — Dashboard Dash]
              └─► 6 abas interativas (refresh a cada 5 min)

A camada de fontes de dados é implementada por um simulador de e-commerce contínuo que replica o comportamento de utilizadores reais a uma taxa de aproximadamente 0,5 sessões por segundo, utilizando dados reais do dataset Olist (produtos, clientes, preços e avaliações históricas) como amostra. O simulador produz dados para três destinos simultâneos, Kafka, PostgreSQL e MinIO, cada um com formato distinto, o que garante a heterogeneidade de fontes característica de um ambiente de produção real.
A ingestão contínua é realizada por três consumers que correm em paralelo e escrevem ficheiros Parquet na camada Bronze do Data Lake; a transformação dos dados é realizada em modo batch horário por jobs Spark que lêem o Bronze e produzem tabelas Apache Iceberg na camada Silver.
A camada de Analitica é orquestrada por uma DAG Airflow com cadência horária que consome as tabelas Silver e produz o modelo dimensional Gold  três dimensões partilhadas e três tabelas de factos em formato Iceberg e de seguida cria ou atualiza seis vistas analíticas Trino sobre esse modelo.
A camada de Machine Learning é orquestrada por uma DAG Airflow independente com cadência diária (03:00 UTC) que treina um modelo Spark MLlib sobre as tabelas Gold e persiste as previsões numa nova tabela Gold consumível pelo dashboard.
A camada de visualização é implementada como uma aplicação Dash que consulta as vistas Trino e as tabelas ML de cinco em cinco minutos e apresenta cinco abas de análise interativa.

3.1.2 Opções de Desenvolvimento
A adoção de uma arquitetura medalhão (Bronze/Silver/Gold) sobre armazenamento de objetos MinIO decorre da natureza heterogénea e contínua das fontes de dados. Os eventos de clickstream chegam como JSON num tópico Kafka sem schema fixo; as ordens de compra chegam via Change Data Capture do WAL do PostgreSQL como eventos de mutação; as avaliações chegam como ficheiros de texto. Nenhum destes formatos pode ser inserido diretamente num modelo analítico sem processamento prévio. A arquitectura em camadas resolve este problema ao separar a preservação do dado original (Bronze) da sua validação e normalização (Silver) e da sua integração no modelo analítico (Gold), tornando cada camada independente e reprocessável sem afetar as restantes.
A escolha do **Apache Iceberg** como formato de tabela para as camadas Silver e Gold justifica-se pela necessidade de garantias ACID num contexto de escrita concorrente. O Iceberg suporta operações MERGE INTO (necessárias para a deduplicação de ordens e avaliações no Silver) e createOrReplace (necessária para a substituição atómica das tabelas ML no Gold) sem corrupção de dados em caso de falha parcial do job. A compatibilidade nativa com Apache Spark e Trino elimina a necessidade de conversão de formato entre camadas.
A escolha do **Apache ****Spark** como motor de processamento justifica-se pela escalabilidade inerente ao modelo de execução distribuída: o mesmo código de transformação funciona correctamente tanto num único nó local como num cluster com dezenas de workers, tornando a migração para um ambiente de maior escala transparente. O uso do módulo Spark MLlib para os modelos de machine learning mantém a consistência tecnológica e evita a necessidade de copiar dados entre frameworks.
A **separação física entre a base de dados operacional** (PostgreSQL, usada pelo simulador e como store do Airflow e Hive Metastore) e o repositório analítico (MinIO/Iceberg + Trino) garante isolamento de carga de trabalho: as queries analíticas pesadas não competem com as escritas do simulador nem com as operações internas do Airflow.
A opção pelo **Trino** como motor de query analítica decorre da necessidade de executar SQL sobre tabelas Iceberg distribuídas em MinIO com desempenho interativo, sem necessidade de carregar os dados para uma base de dados relacional. O Trino actua como camada de abstracção SQL sobre o Data Lake, expondo as tabelas Gold como se fossem tabelas relacionais normais, e suportando a criação de vistas que materializam os joins mais frequentes para consumo pelo dashboard.
3.1.3 Ferramentas Utilizadas
A tabela seguinte apresenta as ferramentas utilizadas no sistema, com a respetiva versão, área de aplicação e justificação.

| **Ferramenta** | **Versão** | **Área de Aplicação** | **Justificação** |
| --- | --- | --- | --- |
| Python | 3.11 | Simulador, ingestão, orquestração | Ecossistema analítico maduro; integração nativa com todas as ferramentas |
| Apache Kafka | 7.6 (KRaft) | Message broker (clickstream) | Desacoplamento produtor/consumidor; garantias de entrega e ordenação por partição |
| Debezium (Kafka Connect) | 2.x | CDC PostgreSQL → Kafka | Captura de mutações no WAL sem alteração do schema operacional |
| Apache Spark | 3.5 | Transformação Bronze→Silver, Silver→Gold, ML | Motor distribuído, suporte nativo a Iceberg e MLlib |
| Apache Iceberg | 1.5 | Formato de tabela Silver e Gold | ACID, schema evolution, time-travel, MERGE INTO nativo |
| Apache Hive Metastore | 3.1.3 | Catálogo de metadados Iceberg | Interface Thrift necessária para Spark + Trino partilharem o mesmo catálogo |
| Trino | 430 | Query engine analítico | SQL interativo sobre Iceberg/MinIO; suporte a vistas; consumível por Dash e BI tools |
| MinIO | RELEASE.2024 | Object Storage (Bronze, Silver, Gold, MLflow) | Compatível com S3 API; self-hosted; buckets particionados por padrão Hive |
| Apache Airflow | 2.x | Orquestração de DAGs | Dependências explícitas entre tasks; backfill; retentativas automáticas; UI de monitorização |
| MLflow | 2.13 | Rastreio de experimentos ML | Logging de parâmetros, métricas e artefactos Spark por categoria/execução |
| Dash + Plotly | 2.x / 5.x | Dashboard interativo | Framework Python para apps web analíticas; consumo directo de Trino via SQL |
| PyArrow | 14.x | Serialização Parquet na ingestão | Escrita vectorizada de Parquet na camada Bronze pelos consumers |
| psycopg2 / SQLAlchemy | 2.9 / 2.x | Conectividade PostgreSQL | Acesso ao Amazon_Sales pelo simulador e ao Airflow metadata DB |
| PostgreSQL | 14 | BD operacional (simulador, Airflow, Hive) | Motor relacional maduro; WAL para CDC via Debezium |
| Docker + Docker Compose | 27.x | Orquestração de containers | Reprodutibilidade completa: 12 serviços numa única rede ge_network |

3.1.4 Equipas, Responsabilidades e Contratos de Interface

A arquitectura do TrendMart não se organiza apenas em camadas tecnológicas — organiza-se também em equipas com âmbitos funcionais estritamente delimitados. Esta divisão não é apenas organizacional: é o mecanismo que garante a independência operacional entre as partes do sistema e torna possível que cada área evolua sem perturbar as restantes. Num pipeline de dados de grande escala, a ausência de fronteiras claras entre equipas é uma das causas mais frequentes de acoplamento técnico indesejado — quando uma equipa altera um componente interno e quebra inadvertidamente o trabalho de outra. A definição explícita de contratos de interface é a forma de prevenir esse acoplamento.

Cada equipa tem responsabilidades bem definidas, materializadas num **contrato de entrada** — o que consome, de quem, com que schema e com que frequência — e num **contrato de saída** — o que entrega, a quem, com que garantias. O sistema funciona correctamente apenas quando cada equipa respeita rigorosamente o seu espaço de trabalho: nem menos, o que quebraria o contrato de saída e impediria as equipas a jusante de operar; nem mais, o que criaria dependências não declaradas e acoplamento entre áreas que devem ser independentes. Uma equipa não acede a artefactos que não constam do seu contrato de entrada, nem produz artefactos fora do seu contrato de saída. O contrato de saída de cada equipa é exactamente o contrato de entrada da equipa seguinte na cadeia — e essa correspondência é o que torna o pipeline coerente como um todo.

O sistema conta com cinco equipas e um sistema simulador. O simulador (`data_sources`) não é uma equipa de dados no sentido técnico — é o componente que substitui a plataforma de e-commerce real que, numa organização em produção, seria o produto. No contexto deste projecto, gera continuamente os dados que alimentam o pipeline a partir do dataset Olist, entregando-os nos três canais que a primeira equipa técnica consome. Por esta razão, é tratado como fonte de dados e não como área funcional — a sua caracterização detalhada encontra-se na secção 3.2.

As cinco equipas organizam-se numa cadeia principal com dois ramos de suporte independentes:

```
data_sources → data_engineering → analytical_engineering → data_analytics
                                          ↑                       ↑
                                    infrastructure          machine_learning
                                    (suporta tudo)         (modelos ML diários)
```

A cadeia principal representa o fluxo de dados — da ingestão à visualização. Os dois ramos de suporte têm papéis distintos: a `infrastructure` não produz nem transforma dados, mas garante que todos os serviços estão operacionais e que os pipelines das restantes equipas são executados com a frequência e as dependências correctas; a `machine_learning` opera de forma independente do pipeline horário, consumindo as tabelas Gold para produzir previsões diárias que reentram no sistema como novos artefactos consumíveis pelo dashboard.

**data_engineering — Ingestão e Transformação Bronze → Silver**

A equipa data_engineering é a primeira camada técnica do pipeline e tem uma responsabilidade dupla: garantir que nenhum evento gerado pelo simulador se perde no trajeto até ao Data Lake, e assegurar que esses dados chegam à camada analítica com qualidade, consistência e rastreabilidade suficientes para sustentar o modelo dimensional construído pela equipa a jusante.

A equipa opera em dois regimes temporais distintos que coexistem em paralelo. O primeiro é a **ingestão contínua**: três consumers correm ininterruptamente, cada um adaptado ao protocolo da sua fonte, e escrevem os dados em bruto na camada Bronze do Data Lake em ficheiros Parquet particionados por data e hora. O segundo é a **transformação horária**: três jobs Spark leem os ficheiros Bronze acumulados, aplicam regras de qualidade e enriquecimento, e produzem tabelas Apache Iceberg na camada Silver — prontas a ser consumidas pela analytical_engineering. A separação entre estes dois regimes é intencional: a ingestão não conhece as regras de qualidade do Silver, e a transformação não conhece os mecanismos de ingestão, tornando os dois processos independentes e substituíveis.

A heterogeneidade das três fontes — streaming de eventos, captura de mutações de base de dados e ficheiros de texto — exige três padrões de ingestão distintos. Cada consumer está adaptado ao protocolo da sua fonte e adiciona metadados de rastreabilidade a cada registo sem alterar o conteúdo original. Na camada Silver, cada job Spark aplica regras de qualidade e deduplicação específicas ao tipo de dado, produzindo tabelas com schema fixo e garantias ACID.

**Contrato de entrada:** tópico Kafka `clickstream_events`, tópico Kafka `debezium.public.simulated_orders`, bucket MinIO `raw-reviews/`.

**Contrato de saída:** três tabelas Apache Iceberg no catálogo `lake`, actualizadas com cadência horária:

| Tabela | Garantias |
| --- | --- |
| `lake.silver.clickstream` | Schema fixo, tipagem forte, sem duplicados por `event_id`, filtrado para 11 tipos de evento válidos |
| `lake.silver.orders` | Validado, enriquecido com `region`, sem duplicados por `order_id` (MERGE INTO ACID) |
| `lake.silver.reviews` | Rating extraído de texto livre, sem duplicados por `review_id` (MERGE INTO ACID) |

A analytical_engineering não acede às fontes originais nem à camada Bronze — consome exclusivamente estas três tabelas. Esta separação torna as duas equipas operacionalmente independentes: a data_engineering pode alterar os seus mecanismos de ingestão ou as suas regras de qualidade sem impacto a jusante, desde que o schema das tabelas Silver se mantenha estável.

A frequência horária das tabelas Silver não é uma decisão interna da data_engineering — é um requisito de SLA imposto pela analytical_engineering e implementado pela equipa infrastructure através da DAG `trendmart_gold_pipeline`. A data_engineering é responsável pelo código; a infrastructure pelo agendamento e execução.

A implementação detalhada desta área — incluindo a estrutura das camadas Bronze e Silver, os mecanismos de checkpoint, particionamento e deduplicação — é descrita na secção 3.3.

**analytical_engineering — Transformação Silver → Gold e Camada Analítica**

A equipa analytical_engineering é responsável pela camada que transforma dados operacionais limpos em informação analítica pronta a consumir. O seu âmbito cobre duas responsabilidades complementares: construir o modelo dimensional Gold — a estrutura de dados que sustenta todas as análises do sistema — e expô-lo ao dashboard através de vistas SQL Trino que encapsulam toda a lógica de agregação, resolução de chaves estrangeiras e cálculo de métricas derivadas.

A equipa consome exclusivamente as tabelas Silver entregues pela data_engineering — nunca acede às fontes originais nem à camada Bronze. Esta separação é o que torna as duas equipas operacionalmente independentes: a analytical_engineering pode evoluir o modelo dimensional sem qualquer dependência da lógica de ingestão, e a data_engineering pode alterar os seus mecanismos internos sem impacto a jusante, desde que os schemas Silver se mantenham estáveis.

O modelo dimensional Gold foi desenhado segundo a metodologia de Kimball, resultando num esquema estrela com três dimensões partilhadas e três tabelas de factos, dimensionado precisamente para os requisitos funcionais identificados no capítulo 2. As tabelas de factos são carregadas de forma incremental por janela horária; as dimensões são recriadas integralmente a cada execução, tornando cada run determinístico e reproduzível.

Sobre o modelo Gold, a equipa cria seis vistas Trino que expõem os dados no formato exactamente esperado pelo dashboard — com joins resolvidos, métricas derivadas calculadas e sem necessidade de transformações no cliente.

**Contrato de entrada:** `lake.silver.clickstream`, `lake.silver.orders`, `lake.silver.reviews`.

**Contrato de saída:**

| Artefacto | Tipo | Padrão de escrita |
| --- | --- | --- |
| `dim_date`, `dim_category`, `dim_geography` | Dimensões Iceberg | `createOrReplace` a cada run |
| `fact_sales`, `fact_clickstream`, `fact_reviews` | Factos Iceberg | Append incremental por janela horária |
| `vw_executive`, `vw_sales_performance`, `vw_funnel`, `vw_reviews`, `vw_trends`, `vw_category_trends` | Vistas Trino | Criadas/substituídas após cada run Gold |

A equipa define o SLA de frequência horária e comunica-o à infrastructure, que o implementa na DAG `trendmart_gold_pipeline`. A analytical_engineering é responsável pelo código dos jobs Spark e das vistas Trino; a infrastructure pela orquestração, dependências e execução.

A implementação detalhada do modelo dimensional, dos padrões de escrita e de cada vista Trino é descrita na secção 3.4.

**machine_learning — Modelos Preditivos e Previsão de Procura**

A equipa machine_learning é responsável pela camada preditiva do sistema. O seu objectivo é transformar o histórico de dados de vendas acumulado nas tabelas Gold em previsões accionáveis de curto prazo — estimando, para cada categoria de produto, o número de ordens esperadas nos sete dias seguintes. Estas previsões permitem à gestão de operações antecipar necessidades de stock, planear campanhas de marketing em categorias com procura crescente e identificar antecipadamente categorias com procura em declínio.

A equipa opera de forma completamente independente do pipeline horário: é orquestrada por uma DAG Airflow própria (`trendmart_ml_pipeline`) com cadência diária às 03:00 UTC, após o pipeline Gold ter os dados do dia completos. Esta separação é intencional — o treino de modelos é computacionalmente mais pesado do que as transformações batch, tem um horizonte temporal de um dia inteiro (não de uma hora), e não deve interferir com a execução do pipeline principal.

O modelo é treinado independentemente por categoria de produto, com features de lag e atributos de calendário, seguindo uma divisão cronológica treino/teste. Cada execução regista os parâmetros e métricas no MLflow, permitindo auditoria e detecção de degradação de desempenho ao longo do tempo.

**Contrato de entrada:** `lake.gold.fact_sales`, `lake.gold.dim_date`, `lake.gold.dim_category` — exclusivamente tabelas Gold. A equipa não acede às camadas Silver nem Bronze.

**Contrato de saída:**

| Tabela | Conteúdo | Padrão de escrita |
| --- | --- | --- |
| `lake.gold.ml_demand_forecast` | Previsões D+1 a D+7 por categoria, com `predicted_orders`, `model_rmse`, `model_mae` e `scored_at` | `createOrReplace` a cada execução diária |

A tabela é consumida directamente pelo dashboard na aba ML Insights e está disponível via Trino tal como as restantes tabelas Gold. A escrita por `createOrReplace` garante que o dashboard apresenta sempre as previsões da execução mais recente, com as métricas do modelo correspondentes.

A implementação detalhada do pipeline de features, do processo de treino, da validação e da avaliação de resultados é descrita no capítulo 5.

**infrastructure — Plataforma e Orquestração**

A equipa infrastructure tem uma posição transversal no sistema: não produz nem transforma dados, mas é a responsável por garantir que todos os serviços estão operacionais, correctamente interligados, e que os pipelines das restantes equipas são executados com a frequência e as dependências certas. Sem a infrastructure, nenhuma outra equipa consegue operar.

As responsabilidades dividem-se em duas categorias distintas. A primeira é a **gestão da plataforma**: definição, construção e arranque de todos os serviços do sistema através de um único ficheiro Docker Compose, com imagens customizadas que garantem que todas as dependências estão pré-instaladas nos containers. A segunda é a **orquestração dos pipelines**: implementação das DAGs Airflow com os schedules, dependências entre tasks, políticas de retentativa e configuração de variáveis de ambiente impostas pelas equipas consumidoras.

Os serviços geridos cobrem todas as camadas tecnológicas do sistema:

| Container | Tecnologia | Função |
| --- | --- | --- |
| `ge_postgres` | PostgreSQL 14 | Base de dados partilhada: `Amazon_Sales` (simulador), `airflow` (metadados), `hive_metastore` |
| `ge_minio` | MinIO | Object storage — buckets Bronze, Silver, Gold, MLflow e raw-reviews |
| `ge_kafka` | Kafka 7.6 KRaft | Message broker para eventos de clickstream |
| `ge_kafka_connect` | Debezium Connect | Conector CDC PostgreSQL WAL → Kafka |
| `ge_hive_metastore` | Hive Metastore 3.1.3 | Catálogo de metadados Iceberg partilhado entre Spark e Trino |
| `ge_trino` | Trino 430 | Motor de query analítico SQL sobre Iceberg/MinIO |
| `ge_airflow_webserver` | Airflow 2.x | Interface de monitorização e gestão de DAGs |
| `ge_airflow_scheduler` | Airflow 2.x | Execução e agendamento das DAGs |
| `ge_mlflow` | MLflow 2.13 | Rastreio de experimentos e artefactos ML |
| `ge_simulator` | Python | Simulador de e-commerce contínuo |
| `ge_minio_init` | Script (one-shot) | Criação inicial dos buckets MinIO |
| `ge_debezium_init` | Script (one-shot) | Registo do conector CDC no Kafka Connect |

A dependência entre serviços é gerida por health checks declarados no Docker Compose, garantindo que o sistema arranca de forma determinística e ordenada sem necessidade de intervenção manual.

No que respeita à orquestração, a infrastructure implementa dois pipelines:

| DAG | Schedule | Responsabilidade |
| --- | --- | --- |
| `trendmart_gold_pipeline` | `0 * * * *` (horária) | Silver + Gold + Vistas Trino — com `max_active_runs=1` para evitar execuções paralelas sobre o mesmo catálogo Iceberg |
| `trendmart_ml_pipeline` | `0 3 * * *` (diária, 03:00 UTC) | Modelos ML — corre após o Gold do dia estar completo |

Os schedules não são decisões da infrastructure — são requisitos de SLA comunicados pelas equipas consumidoras (a analytical_engineering impõe a cadência horária do Gold; a machine_learning impõe a cadência diária do ML) e implementados pela infrastructure como contratos operacionais. A infrastructure é também responsável por garantir que os volumes Docker montam o código de cada equipa nos paths esperados pelos containers Airflow, tornando as actualizações de código transparentes sem necessidade de reconstruir imagens.

**data_analytics — Visualização e Interface de Análise**

A equipa data_analytics é a consumidora final do pipeline e o único ponto de contacto entre o sistema de dados e os utilizadores de negócio. A sua responsabilidade é expor os dados analíticos produzidos pelas equipas a montante num dashboard interativo acessível, sem escrever dados, sem transformar registos e sem conhecimento da arquitectura interna do pipeline.

O produto da equipa é uma aplicação web Plotly Dash com Bootstrap, organizada em seis abas temáticas, cada uma orientada a um perfil de utilizador distinto definido no capítulo 2. A aplicação actualiza automaticamente os seus dados a cada cinco minutos via `dcc.Interval`, garantindo que a informação apresentada reflecte sempre o estado mais recente do pipeline Gold sem necessidade de intervenção manual.

A aplicação está estruturada com separação entre a camada de dados e a camada de apresentação, garantindo que a lógica de acesso a dados pode evoluir independentemente da camada de visualização.

A adopção das vistas Trino como interface de dados tem uma consequência arquitectural central: o dashboard é um consumidor passivo. Recebe dados já agregados, com joins resolvidos e métricas derivadas calculadas — sem necessidade de lógica de negócio embutida no cliente. Qualquer alteração ao modelo dimensional é absorvida nas vistas sem impacto no dashboard; as mesmas vistas podem ser consumidas por outras ferramentas de BI sem alteração da camada de dados.

**Contrato de entrada:** as seis vistas Trino do schema `lake.gold` e a tabela `lake.gold.ml_demand_forecast`.

| Aba | Vista(s) consumida(s) | Perfil | Janela temporal |
| --- | --- | --- | --- |
| Executivo | `vw_executive` | Gestor Executivo | 90 dias |
| Vendas | `vw_sales_performance` | Analista de Vendas | 90 dias |
| Funil | `vw_funnel` | Analista de Marketing | 7 dias |
| Reviews | `vw_reviews` | Gestor de Customer Experience | 90 dias |
| Tendências | `vw_trends` + `vw_category_trends` | Analista de Tendências | 60 / 30 dias |
| ML Insights | `ml_demand_forecast` | Data Scientist | Próximos 7 dias |

A equipa não tem contrato de saída no sentido técnico — o produto que entrega é a interface de análise em si, acessível em `http://localhost:8050`. A implementação detalhada de cada aba, as visualizações e o seu valor analítico são descritos no capítulo 4.

3.2 Fontes de Dados
O sistema Data Sources constitui o ponto de entrada do pipeline TrendMart. O enunciado desta unidade curricular exige que o sistema integre dados provenientes de fontes heterogéneas, incluindo obrigatoriamente dados históricos de vendas e pelo menos uma fonte adicional de natureza distinta. O TrendMart satisfaz e ultrapassa este requisito ao integrar três fontes de dados estruturalmente diferentes, cobrindo o espectro completo de padrões de ingestão presentes em arquiteturas analíticas de e-commerce reais: dados relacionais estruturados, eventos semi-estruturados em JSON e texto não estruturado.
Na ausência de uma plataforma de e-commerce em produção, as três fontes são geradas por um simulador contínuo que replica o comportamento de uma loja digital. O simulador parte de uma base comum de referência: os CSVs do dataset Olist Brazilian E-Commerce (produtos, clientes, preços históricos e avaliações reais), carregados em memória na inicialização através do módulo loader.py, o que garante coerência referencial entre as três fontes — os produtos que surgem nos eventos de clickstream são os mesmos que originam ordens de compra e avaliações.

**Objetivos do sistema Data ****Sources**

A escolha e o desenho destas três fontes assentam em três objetivos deliberados, alinhados com os requisitos do enunciado:

Realismo
Cada fonte replica mecanismos e padrões de dados que existem em ambientes de e-commerce reais. O clickstream usa Apache Kafka, o protocolo de facto para ingestão de eventos de comportamento a alta frequência. As ordens de compra chegam via Change Data Capture (CDC) com Debezium, replicando a forma como sistemas analíticos reais consomem alterações de bases de dados transacionais sem acesso direto à base de dados de produção. As avaliações são persistidas como ficheiros de texto em object storage (MinIO), simulando integrações com sistemas externos que depositam dados via S3. O simulador injeta ainda ruído intencional em cada fonte — preços inválidos, encoding corrompido, duplicados CDC, campos nulos — para replicar as imperfeições típicas de sistemas distribuídos reais e justificar a existência de uma camada de preparação de dados.

Heterogeneidade
As três fontes diferem propositalmente em estrutura, protocolo e semântica. O clickstream é semi-estruturado — o campo properties de cada evento varia consoante o tipo de interação, impossibilitando modelação rígida em esquemas relacionais. As ordens são totalmente estruturadas, com schema fixo e garantias transacionais. As avaliações são não estruturadas — texto livre em português, sem campos delimitados, onde a metadata está apenas no nome do ficheiro. Esta diversidade estrutural — base de dados relacional, JSON e texto livre — é o que justifica a adoção de uma arquitetura Data Lake com camadas progressivas de preparação, em vez de uma solução puramente relacional.

Grande escala
O clickstream gera o volume mais expressivo: cada sessão produz múltiplos eventos sequenciais, resultando numa produção contínua de dados comportamentais a ~0,5 sessões/segundo. Este padrão de alta frequência justifica o uso de Apache Kafka como broker assíncrono e representa, mesmo em ambiente académico controlado, os padrões de "data at scale" que motivam arquiteturas de streaming em produção.

**Fluxo de dados e responsabilidades**

O sistema Data Sources tem uma responsabilidade bem delimitada: receber os CSVs Olist como referência e entregar dados nos três pontos finais abaixo. A partir desses pontos, a responsabilidade passa integralmente para a equipa de Engenharia de Dados.

| # | Ponto Final | Sistema | Destino Concreto |
| --- | --- | --- | --- |
| 1 | Clickstream de Navegação | Apache Kafka | Tópico clickstream_events |
| 2 | Ordens de Compra | PostgreSQL + Debezium CDC | Tabela simulated_orders → tópico CDC Kafka |
| 3 | Avaliações de Clientes | MinIO (object storage) | Bucket raw-reviews/{YYYY-MM-DD}/{review_id}_{order_id}.txt |

A tabela seguinte apresenta uma caracterização sumária das três fontes.

| Fonte | Protocolo de Transporte | Formato | Periodicidade | Tipo |
| --- | --- | --- | --- | --- |
| Clickstream de Navegação | Apache Kafka | JSON (mensagem) | Contínuo (~0,5 sess/s) | Semi-estruturado |
| Ordens de Compra | Debezium CDC (PostgreSQL WAL) | JSON (evento CDC) | Contínuo (por inserção) | Estruturado |
| Avaliações de Clientes | MinIO polling (ficheiros .txt) | Texto com cabeçalho | Contínuo (por compra) | Semi-estruturado |

3.2.1 Clickstreams de Navegação

**Descrição e Caracterização**

O clickstream de navegação corresponde ao registo sequencial de interações realizadas pelos utilizadores durante a utilização de uma plataforma de e-commerce. Cada registo representa uma ação individual dentro de uma sessão, captando a evolução do comportamento do utilizador ao longo do processo de navegação.
Esta fonte permite observar a totalidade da jornada do utilizador, incluindo fases de descoberta, exploração de produtos, interação com o carrinho e eventual abandono da sessão ou conclusão de compra. Ao contrário de dados transacionais, que apenas refletem resultados finais, o clickstream representa o comportamento em curso, independentemente da existência de conversão.
Cada sessão é composta por uma sequência ordenada de eventos associados a um único utilizador, permitindo a reconstrução da trajetória de navegação e a análise do comportamento ao nível do funil de conversão.
Os eventos seguem uma estrutura comum composta por atributos de identificação, contexto temporal e classificação da interação, incluindo o tipo de evento e um conjunto de propriedades associadas ao contexto da ação.
Os principais tipos de evento incluem interações de descoberta (pesquisa e navegação por categoria), exploração de produto, interação com o carrinho e encerramento de sessão. Esta diversidade de eventos permite representar diferentes fases do comportamento do utilizador dentro de uma mesma fonte de dados.

**Justificação e Utilidade**

A inclusão da fonte de clickstream no sistema TrendMart resulta de uma decisão deliberada de modelação de dados orientada à diversidade e realismo de um pipeline moderno de e-commerce. Em sistemas reais, o comportamento do utilizador é capturado de forma contínua através de eventos de navegação, sendo esta uma das componentes mais representativas de plataformas digitais atuais.
No contexto deste projeto, os clickstreams foram escolhidos como mecanismo central de geração de eventos comportamentais por três razões principais. Em primeiro lugar, introduzem heterogeneidade estrutural nos dados, uma vez que os eventos apresentam uma estrutura semi-estruturada, em que o campo de propriedades (properties) varia consoante o tipo de interação. Esta característica aproxima o sistema de arquiteturas reais baseadas em event tracking e impossibilita a modelação rígida em esquemas relacionais tradicionais, reforçando a necessidade de uma abordagem orientada a Data Lake.
Em segundo lugar, esta fonte simula um cenário de alta frequência de eventos e grande escala de dados, ainda que inserido num ambiente mais simples e controlado. Cada sessão gera múltiplos eventos sequenciais, resultando num volume significativamente superior ao das fontes transacionais. Esta escolha é particularmente relevante no contexto da unidade curricular, uma vez que permite representar padrões típicos de sistemas de “data at scale”, justificando o uso de mecanismos de ingestão como streaming através de message brokers (por exemplo, Apache Kafka), capazes de lidar com produção contínua e assíncrona de dados.
Em terceiro lugar, os clickstreams permitem múltiplas perspetivas analíticas sobre o comportamento do utilizador ao longo do funil de conversão. Ao contrário de dados transacionais, que apenas capturam resultados finais, esta fonte permite observar o processo completo de decisão, incluindo navegação, exploração de produtos, interação com o carrinho e abandono de sessão. Isto possibilita análises mais ricas, como identificação de pontos de fricção, padrões de abandono e segmentação comportamental baseada em trajetórias de navegação.
Adicionalmente, a escolha desta fonte foi influenciada pela sua elevada representatividade em sistemas de e-commerce contemporâneos, onde a instrumentação de eventos de navegação constitui uma prática padrão em web analytics e sistemas de recomendação. Assim, os clickstreams desempenham um papel central na aproximação do sistema a arquiteturas reais, funcionando como base para o consumo por diferentes camadas analíticas do pipeline.

**Criação / Geração**

A geração dos eventos de clickstream é realizada pelo módulo simulator/producer.py, responsável por simular o comportamento de utilizadores e publicar eventos em tempo real no Kafka. 
O processo de geração inicia-se na função simulate_session() (definida em session.py), que modela o comportamento do utilizador ao longo de um funil probabilístico de navegação. Esta função devolve três possíveis saídas:
lista de eventos de clickstream
registo de compra (opcional)
avaliação de cliente (opcional)
Os eventos são posteriormente publicados em tempo real no tópico Kafka clickstream_events através do producer confluent_kafka.

Ordem temporal e consistência por sessão
Para garantir a preservação da ordem causal dos eventos, o session_id é utilizado como chave de partição no Kafka. Esta decisão garante que:
todos os eventos de uma sessão são escritos na mesma partição;
a ordem de publicação é preservada;
a reconstrução da sequência temporal é determinística no consumo.
Este mecanismo é essencial para suportar análises de funil de conversão, onde a ordem dos eventos é semanticamente relevante.

Modelo de dados em memória
O simulador mantém um conjunto de entidades em memória (produtos, clientes e avaliações) carregadas no arranque a partir do dataset Olist Brazilian E-Commerce através do módulo loader.py.
Esta abordagem elimina dependências de I/O durante a geração de eventos e garante:
consistência referencial entre eventos;
repetibilidade controlada da simulação;
redução de latência na geração contínua de eventos.

Estrutura dos Eventos JSON

Cada evento de clickstream segue uma estrutura JSON uniforme que combina atributos de identificação, contexto temporal e informação específica da interação. Esta estrutura foi desenhada para suportar tanto processamento em streaming como análises analíticas em batch, permitindo reconstruir sessões completas de utilizadores e correlacionar comportamentos com eventos transacionais posteriores. 
A estrutura base de cada evento inclui os seguintes campos:


| **Campo** | **Descrição** |
| --- | --- |
| event_id | Identificador único do evento, utilizado para rastreabilidade e deduplicação |
| session_id | Identificador da sessão de navegação, comum a todos os eventos da mesma visita |
| user_id | Identificador do utilizador associado à sessão |
| event_type | Tipo de interação realizada no funil de navegação |
| timestamp | Momento de geração do evento em UTC |
| device | Tipo de dispositivo utilizado (mobile, desktop ou tablet) |
| properties | Objeto JSON com atributos específicos do tipo de evento |

Os campos base de cada evento são gerados pela função _event() no módulo session.py, garantindo uniformidade estrutural em todo o pipeline. Entre os campos principais encontram-se identificadores únicos como event_id, session_id e user_id, que permitem rastreabilidade total e associação inequívoca entre eventos e entidades. O campo timestamp, registado em formato ISO UTC, assegura a correta ordenação temporal dos eventos independentemente do sistema consumidor.
O campo device introduz contexto adicional sobre o tipo de dispositivo utilizado pelo utilizador (mobile, desktop ou tablet), permitindo análises de segmentação comportamental. Já o campo event_type identifica o tipo de interação realizada no funil de navegação, sendo o principal discriminador semântico dos eventos.
O elemento mais flexível da estrutura é o campo properties, um objeto JSON de natureza semi-estruturada cuja composição varia consoante o tipo de evento. Esta decisão arquitetural permite representar diferentes tipos de interação sem impor um esquema rígido na fase de ingestão, suportando assim a heterogeneidade típica de sistemas de eventos.

A Tabela seguinte apresenta exemplos representativos do conteúdo deste campo.

| **event_type** | **Exemplos de propriedades** |
| --- | --- |
| search | query, category |
| product_view | product_id, category, price |
| add_to_cart | product_id, price |
| cart_view | product_id, total |
| checkout_start | product_id, total |
| session_end | reason |

Por exemplo, eventos do tipo search incluem campos como query e category, enquanto eventos como product_view incluem atributos mais ricos como product_id, price e photos_qty. Já eventos de checkout_start ou cart_view incluem valores agregados como o total da compra. 

Esta variabilidade estrutural justifica a adoção de uma abordagem schema-on-read na camada Silver do pipeline, onde os eventos são posteriormente normalizados e transformados num esquema analítico consistente utilizando Apache Spark. 

Funil de Conversão

O comportamento dos utilizadores é modelado através de um funil de conversão probabilístico implementado no módulo session.py, que simula a progressão típica de uma sessão num sistema de comércio eletrónico. Cada sessão é iniciada com um evento de session_start, seguido de uma decisão inicial entre duas formas de descoberta: pesquisa direta (search) ou navegação por categoria (category_browse).
A partir deste ponto, o fluxo de eventos segue uma lógica probabilística condicional que determina a evolução da sessão. Uma parte significativa das sessões termina precocemente através de um evento de session_end com razão bounce, simulando utilizadores que abandonam a plataforma sem interagir com produtos. As sessões que continuam evoluem para product_view, onde é introduzida informação detalhada do produto, incluindo preço, categoria e número de imagens disponíveis.
Nesta fase, pode ocorrer um evento opcional de product_review_read, que simula a consulta de avaliações por parte do utilizador, refletindo o impacto do social proof na decisão de compra. Caso o utilizador demonstre maior intenção de compra, pode ocorrer a transição para add_to_cart, representando um sinal forte de intenção transacional.
O simulador inclui ainda comportamentos realistas de hesitação, como remove_from_cart seguido de nova adição, bem como eventos intermédios de cart_view, onde o utilizador avalia o custo total da compra. A partir daqui, uma parte das sessões é perdida através de cart_abandon, enquanto outras avançam para checkout_start.
No estágio final do funil, ocorre a decisão de compra, modelada pelo evento order_placed, que representa uma conversão efetiva. Este evento é deliberadamente separado do fluxo Kafka e enviado diretamente para a base de dados PostgreSQL, permitindo a integração entre dados de comportamento (streaming) e dados transacionais (persistência estruturada). A sessão termina com um evento session_end, que codifica o motivo do encerramento, permitindo segmentação detalhada de abandonos e conversões.

 O diagrama seguinte representa o funil completo implementado em `session.py`:

session_start
 	│
 	├─► search (60%)  ──────────────────────────────────────────────┐
 	└─► category_browse (40%) ──────────────────────────────────────┤
                                                                      │
     ┌────────────────────────────────────────────────────────────────┘
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

A taxa de conversão global implícita neste modelo é aproximadamente 10,8%, valor obtido pela multiplicação das probabilidades condicionais do funil e alinhado com benchmarks reais de e-commerce. 
A taxa de conversão global implícita nestas probabilidades é de aproximadamente 10,8%: apenas cerca de 1 em cada 10 sessões resulta numa encomenda concluída (0,70 × 0,35 × 0,55 × 0,80 ≈ 0,108). Este valor está alinhado com benchmarks reais de e-commerce, onde taxas de conversão entre 2% e 15% são típicas consoante o segmento de produto e canal de aquisição.

Cada etapa do funil tem um significado analítico distinto:

- **`session_start`**: marca o início da sessão; contém o contexto geográfico do utilizador (estado e cidade). É o denominador de todas as métricas de conversão.
- **`search`**: o utilizador inicia a jornada por pesquisa de texto — o termo de pesquisa é o nome da categoria do produto amostrado, simulando comportamento de busca dirigida.
- **`category_browse`**: alternativa ao search; o utilizador navega diretamente por uma categoria — simula descoberta por navegação em vez de intenção explícita de pesquisa.
- **`product_view`**: registo da visualização de um produto específico; inclui preço e número de fotografias do produto, que são fatores de decisão de compra com relevância analítica.
- **`product_review_read`**: evento opcional que ocorre quando o utilizador consulta as avaliações de outros compradores antes de decidir; a sua frequência (40%) reflete a importância do social proof no comportamento de compra digital.
- **`add_to_cart`**: o utilizador coloca o produto no carrinho — sinal de intenção de compra forte, mas ainda reversível.
- **`remove_from_cart`**: o utilizador remove o produto do carrinho (15% dos casos) antes de o voltar a adicionar — simula hesitação no processo de decisão.
- **`cart_view`**: visualização do conteúdo do carrinho com o total calculado (produto + frete); momento de avaliação final antes do checkout.
- **`cart_abandon`**: o utilizador abandona o carrinho sem prosseguir para o checkout — uma das métricas de maior valor em e-commerce, pois identifica fricção específica no momento de confirmação da compra.
- **`checkout_start`**: o utilizador inicia o processo de checkout — intenção de compra muito forte; o abandono a partir daqui (20%) é tipicamente atribuível a problemas no processo de pagamento.
- **`session_end`**: fecha a sessão com um campo `reason` que codifica o motivo de encerramento (`bounce`, `no_cart`, `cart_abandon`, `checkout_abandon`, `completed`), permitindo segmentar os abandonos por etapa sem necessidade de joins adicionais.

Note-se que o evento `order_placed` é gerado internamente pelo simulador mas **não é publicado no tópico Kafka** é encaminhado exclusivamente para o PostgreSQL via `db_writer.py`. Esta separação é intencional, a análise de conversão no dashboard é feita correlacionando os eventos `checkout_start` do Kafka com as ordens confirmadas no PostgreSQL.

Ruído e Anomalias de Qualidade

De forma a aproximar o comportamento do sistema de cenários reais de produção, o simulador introduz perturbações controladas nos eventos de clickstream através do módulo simulator/noise.py. Estas anomalias simulam problemas comuns em pipelines de web analytics, incluindo perda de identificadores de sessão, erros de instrumentação e tráfego não-humano.

As principais categorias de ruído introduzidas são:
ausência de session_id, simulando falhas de persistência de sessão ou bloqueadores de rastreamento;
valores inválidos no campo device, representando bots, crawlers ou clientes não normalizados;
ausência de event_type, simulando falhas de instrumentação no cliente de tracking.

No caso dos clickstreams, são introduzidas anomalias como a ausência de session_id, valores inválidos no campo device (por exemplo bots ou crawlers) e eventos sem event_type, simulando falhas de instrumentação no lado do cliente. Estas alterações são aplicadas com probabilidades reduzidas, garantindo que a maior parte dos dados permanece válida, mas suficiente para testar mecanismos de tolerância a erros.
Na camada Silver, estes eventos são sujeitos a validações rigorosas, sendo descartados quando não possuem identificadores essenciais ou quando não cumprem requisitos mínimos de integridade. Valores inválidos são normalizados para um vocabulário controlado, assegurando consistência antes da transformação para o modelo analítico final.
No caso das ordens e reviews, o sistema introduz ruído adicional mais diversificado, incluindo preços inválidos, estados mal formatados, problemas de encoding em mensagens de texto e duplicação de eventos via reprocessamento de CDC. Estes cenários simulam problemas comuns em sistemas distribuídos reais, como falhas de escrita, reentregas de eventos e inconsistências de encoding entre serviços.

As perturbações são aplicadas com probabilidades controladas e de forma independente, permitindo gerar dados imperfeitos semelhantes aos observados em ambientes reais de navegação web.
O objetivo desta abordagem não é apenas aumentar o realismo da simulação, mas também justificar a existência de uma camada intermédia de preparação de dados, responsável por garantir qualidade, consistência e fiabilidade antes da integração na camada Gold do pipeline analítico. 

3.2.2 Ordens de Compra

**Descrição e Caracterização**
A fonte de ordens de compra corresponde ao registo das transações concluídas pelos utilizadores na plataforma de e-commerce. Cada registo representa uma compra efetivamente realizada, ou seja, o resultado final de um processo de navegação que culmina na conversão.
Estas ordens constituem os dados centrais de natureza transacional do sistema, refletindo decisões de compra já concretizadas. Ao contrário de fontes comportamentais, como o clickstream, que capturam a interação do utilizador ao longo da sessão, as ordens representam apenas o estado final do processo de compra.
Cada registo de compra contém informação essencial sobre a transação, incluindo o identificador da ordem, o cliente associado, o produto adquirido, a categoria, o preço pago, os custos de envio e a localização do cliente. Adicionalmente, inclui também o momento em que a compra foi realizada, permitindo análises temporais de vendas.
Esta fonte é, por natureza, estruturada e altamente consistente, uma vez que cada transação segue um conjunto fixo de atributos que descrevem o resultado final da interação do utilizador com a plataforma.

**Justificação e Utilidade**
A fonte de ordens de compra é fundamental em qualquer sistema de análise de e-commerce, uma vez que representa o ponto de conversão efetiva da atividade dos utilizadores. Enquanto outras fontes descrevem intenções ou comportamentos intermédios, as ordens correspondem a eventos de negócio concluídos.
Em ambientes reais de comércio eletrónico, este tipo de dados é utilizado como referência principal para métricas de desempenho, como volume de vendas, receita, ticket médio e taxas de conversão. Sem esta fonte, não seria possível avaliar o impacto real das interações dos utilizadores na geração de receita.
No contexto do sistema TrendMart, as ordens permitem complementar a informação comportamental proveniente do clickstream, possibilitando a análise completa do funil de conversão, desde a navegação até à compra final. Esta ligação entre comportamento e resultado é essencial para compreender a eficácia da plataforma.
Adicionalmente, esta fonte permite análises de negócio baseadas em categorias de produto, distribuição geográfica e evolução temporal das vendas, fornecendo uma visão consolidada da atividade comercial.
Por fim, as ordens de compra representam o principal ponto de ligação entre os dados operacionais e os indicadores analíticos do sistema, servindo como base para a construção de métricas agregadas na camada Gold.

**Criação e Geração**

A geração de ordens de compra no sistema não ocorre de forma isolada, mas sim como parte integrada da simulação de sessão do utilizador. Tanto os eventos de clickstream como a eventual criação de uma ordem resultam da execução da função simulate_session(), implementada no módulo simulator/session.py.
Esta função representa o núcleo lógico da simulação comportamental do utilizador, reproduzindo o percurso completo do funil de conversão de forma sequencial e determinística. Durante a execução, são gerados eventos de interação (clickstream), sendo simultaneamente avaliado se a sessão culmina numa conversão. Caso a conversão ocorra, é produzido um registo de ordem de compra associado; caso contrário, a sessão termina sem transação.
O resultado da função é devolvido sob a forma de um triplo estruturado composto por:
sequência de eventos de clickstream
registo de compra (opcional)
uma avaliação do cliente (opcional)
Esta abordagem garante coesão entre comportamento e resultado, permitindo manter consistência entre os diferentes tipos de dados gerados.

O processo de execução principal, definido em main.py, consome este output e distribui os dados por diferentes camadas do sistema. Os eventos de clickstream são publicados em tempo real no Apache Kafka, enquanto as ordens de compra são persistidas diretamente numa base de dados PostgreSQL. Esta separação reforça o princípio de heterogeneidade dos dados, uma vez que diferentes tipos de informação são encaminhados por canais distintos.
Ambos os registos partilham identificadores comuns (session_id e order_id), o que permite estabelecer relações determinísticas entre comportamento e transação. Esta ligação é fundamental para análises posteriores de funil e atribuição.
Adicionalmente, o evento lógico order_placed é gerado internamente durante a simulação como marcador de conversão, mas não é publicado no Kafka. Em vez disso, serve apenas como gatilho interno para a criação da ordem na base de dados.
A integração com a camada de ingestão é assegurada através de Change Data Capture (CDC), implementado com Debezium. O conector monitoriza continuamente a tabela simulated_orders no PostgreSQL e captura todas as alterações ao nível do log transacional (WAL). Estas alterações são publicadas automaticamente num tópico Kafka dedicado, permitindo a propagação assíncrona dos dados para a camada de processamento.
Posteriormente, um consumer especializado processa estes eventos e escreve os dados na camada Bronze em formato Parquet, garantindo a persistência analítica e a compatibilidade com as fases subsequentes do pipeline.

Estrutura do Registo

Cada registo de ordem de compra segue uma estrutura fixa e normalizada, concebida para garantir consistência e facilitar o processamento analítico. Os principais campos incluem identificadores de negócio, atributos descritivos do produto e informação temporal associada à transação.


| Campo | Descrição |
| --- | --- |
| order_id | Identificador único da transação |
| session_id | Identificador da sessão de navegação associada à compra |
| customer_id | Identificador do cliente |
| product_id | Identificador do produto adquirido |
| seller_id | Identificador do vendedor |
| category | Categoria do produto |
| price | Valor do produto no momento da compra |
| freight_value | Custo de envio |
| state | Estado do cliente |
| purchase_timestamp | Momento da transação |

Para além destes atributos, o mecanismo de CDC introduz metadados adicionais associados às mutações da base de dados, incluindo o tipo de operação realizada (create ou update) e timestamps de origem do evento. Estes campos são relevantes para rastreabilidade do pipeline, mas são descartados na camada de processamento analítico, onde apenas os atributos de negócio são preservados.

Ruído e Anomalias de Qualidade

De forma a aproximar o sistema de um ambiente real de produção, são introduzidas deliberadamente anomalias controladas na geração das ordens de compra. Esta estratégia é essencial para simular imperfeições típicas de sistemas distribuídos e garantir robustez nas camadas posteriores de tratamento e validação de dados.
Uma das principais anomalias introduzidas consiste na presença de valores de estado mal formatados em aproximadamente 5% dos registos. Nestes casos, o código de estado normalizado é substituído por variantes não padronizadas, resultantes de manipulação aleatória do formato original (por exemplo, variações de capitalização ou concatenação de caracteres adicionais). Estes casos são posteriormente corrigidos na camada de processamento, onde regras de normalização garantem consistência semântica.
Outra fonte relevante de ruído decorre do comportamento do mecanismo de Change Data Capture. Dado o modelo de entrega “at-least-once” do Debezium, podem ocorrer duplicações de eventos associadas a atualizações redundantes da mesma ordem. Estas situações são simuladas através de operações de atualização sem alteração efetiva de dados (noop updates), levando à emissão de múltiplos eventos para o mesmo order_id.
Este comportamento não é considerado erro, mas sim uma característica intrínseca de sistemas baseados em logs de transações. Assim, a camada de processamento posterior é responsável pela deduplicação e garantia de consistência final dos dados na camada analítica.

3.2.3 Avaliações de Clientes (MinIO Polling)

**Descrição e Caracterização**
A fonte de avaliações de clientes simula o feedback textual deixado pelos utilizadores após a receção das suas encomendas numa plataforma de e-commerce. Esta fonte representa uma dimensão qualitativa do sistema, complementando os dados comportamentais (clickstream) e transacionais (ordens de compra), ao introduzir a perceção subjetiva da experiência de compra.
As avaliações são geradas pelo módulo simulator/review_writer.py, que associa probabilisticamente uma review a aproximadamente 40% das ordens concluídas. A geração ocorre com um atraso controlado entre 30 e 300 segundos após a criação da ordem, simulando o comportamento real de utilizadores que apenas avaliam produtos após receção e utilização inicial.
Cada avaliação é persistida como um ficheiro .txt no bucket MinIO raw-reviews/, utilizando o identificador da ordem como chave primária ({order_id}.txt). Este mecanismo permite garantir rastreabilidade direta entre transação e feedback do cliente.
O formato de cada ficheiro segue uma estrutura híbrida:
Um cabeçalho estruturado, contendo:
order_id
customer_id
rating (escala de 1 a 5)
timestamp
Um corpo de texto livre, composto por comentários em português, amostrados a partir de avaliações reais do dataset Olist Brazilian E-Commerce.
Esta estrutura reflete padrões comuns em sistemas reais de feedback, onde formulários estruturados coexistem com texto livre não normalizado, exigindo processamento posterior de linguagem natural.

**Justificação e Utilidade**
A inclusão desta fonte no sistema TrendMart permite incorporar uma dimensão essencial frequentemente ausente em pipelines puramente transacionais: a qualidade percebida pelo utilizador.
Enquanto o clickstream descreve comportamento e as ordens representam conversão, as avaliações introduzem uma métrica de satisfação, permitindo análises como:
relação entre categoria de produto e satisfação do cliente;
impacto da entrega (freight_value) na perceção do serviço;
correlação entre abandono de sessão e avaliações negativas;
identificação de produtos com elevado volume de vendas mas baixa satisfação.
Do ponto de vista arquitetural, esta fonte introduz um terceiro padrão de ingestão distinto:
Kafka → streaming em tempo real (clickstream)
CDC via Debezium → captura de alterações em base de dados (ordens)
MinIO polling → ingestão batch incremental baseada em ficheiros
Este último padrão é particularmente relevante em cenários reais, onde dados externos ou legados são frequentemente disponibilizados através de armazenamento de objetos em vez de APIs ou streams.

**Criação / Geração**
A geração das avaliações é realizada de forma desacoplada do fluxo principal de eventos. O módulo review_writer.py subscreve logicamente o resultado da função simulate_session(), criando avaliações apenas para ordens efetivamente concluídas.

O processo inclui um atraso artificial entre a compra e a escrita do ficheiro, simulando o tempo real de receção do produto. A taxa de geração (≈40%) e o delay variável introduzem variabilidade temporal semelhante à observada em sistemas reais de e-commerce.
A ingestão é realizada pelo componente file_watcher.py, que monitoriza periodicamente o bucket MinIO raw-reviews/ com uma frequência fixa de 30 segundos.
O sistema mantém um estado persistente (.watcher_state.json) contendo os ficheiros já processados, garantindo idempotência e evitando reprocessamento de dados.
Sempre que novos ficheiros são detetados:
são lidos via API S3 compatível do MinIO;
os metadados são extraídos e normalizados;
o texto é armazenado na camada Bronze em formato Parquet;
são adicionados campos de controlo como:
ingested_at
source
file_name

3.3 Área de Preparação de Dados
A área de preparação de dados é da responsabilidade da equipa data_engineering, cujo âmbito cobre duas fases distintas do pipeline: a ingestão contínua de dados provenientes das três fontes do simulador e a sua transformação progressiva em representações analíticas limpas, tipadas e semanticamente enriquecidas. O objectivo global da equipa é garantir que os eventos gerados pelo simulador chegam ao sistema de análise sem perda, com metadados de rastreabilidade e com garantias de qualidade suficientes para sustentar o modelo dimensional construído pela equipa a seguinte – Engenheiros Analíticos.
O fluxo de dados tem origem na equipa data_sources, que entrega dados em três formatos e protocolos distintos: eventos de clickstream como mensagens JSON no tópico Kafka clickstream_events; registos de ordens de compra como eventos CDC no tópico Kafka debezium.public.simulated_orders, produzidos pelo Debezium a partir do WAL do PostgreSQL; e avaliações de clientes como ficheiros de texto não estruturado no bucket MinIO raw-reviews/. Estes três canais constituem o contrato de entrada da equipa data_engineering — nenhum dado é consumido de outra origem.
No final do seu processamento, a equipa de engenharia de dados entrega à equipa de engenharia analítica três tabelas Apache Iceberg na camada Silver, registadas no catálogo lake através do Hive Metastore e acessíveis via Trino e Spark. Este conjunto de tabelas constitui o contrato de saída da equipa:
lake.silver.clickstream — eventos de clickstream desserializados, com schema fixo e tipagem forte, deduplicados por event_id e filtrados para os 11 tipos de eventos válidos do funil de conversão;
lake.silver.orders — ordens de compra validadas, enriquecidas com o campo region derivado do estado do cliente, e deduplicadas por order_id com garantias ACID via MERGE INTO Iceberg;
lake.silver.reviews — avaliações com rating extraído por expressão regular a partir do texto livre, deduplicadas por review_id.
A equipa analytical_engineering não acede às fontes originais nem à camada Bronze — consome exclusivamente as tabelas Silver entregues por este contrato. Esta separação é o que torna as duas equipas operacionalmente independentes: a data_engineering pode alterar os seus mecanismos de ingestão ou as suas regras de qualidade sem quebrar o contrato de saída desde que o schema das tabelas Silver se mantenha estável; a analytical_engineering pode evoluir o seu modelo dimensional Gold sem qualquer dependência da lógica de ingestão.

3.3.1 Justificação da Abordagem
A área de preparação de dados do projeto é implementada segundo uma arquitectura Data Lakehouse com padrão Medallion (Bronze / Silver / Gold), sobre armazenamento de objetos MinIO. O Data Lakehouse combina as características de um Data Lake — capacidade de receber qualquer dado no seu formato original, sem schema obrigatório na entrada — com as características de um Data Warehouse — garantias ACID, schema enforced e capacidade de consulta SQL analítica. No TrendMart esta divisão manifesta-se de forma explícita por camadas: o Bronze é a componente "lake", onde os dados chegam em bruto e sem transformação; o Silver e o Gold são a componente "house", onde os dados adquirem schema fixo, tipagem forte, garantias transacionais via Apache Iceberg e capacidade de consulta SQL via Trino. As características específicas de cada camada são detalhadas nas secções 3.3.2 e 3.3.3.
A adopção desta arquitectura decorre da natureza heterogénea das três fontes de dados — eventos JSON num tópico Kafka, eventos CDC com envelope Debezium, e ficheiros de texto com formato misto — que torna impossível a inserção directa num schema relacional fixo sem processamento prévio. Um sistema puramente relacional exigiria schema definido na entrada, incompatível com a variabilidade do envelope CDC e com a estrutura não estruturada dos ficheiros de review. A arquitectura Lakehouse resolve este problema ao aceitar qualquer dado na camada Bronze sem imposição de schema, diferindo essa imposição para a camada Silver, e só então promovendo os dados para o modelo analítico Gold.
A separação progressiva em camadas traz três vantagens concretas: 
(1) rastreabilidade — em caso de erro de transformação, os dados originais estão sempre preservados no Bronze, permitindo reprocessamento sem re-ingestão da fonte; 
(2) idempotência — o Spark Structured Streaming mantém um checkpoint que regista os ficheiros Bronze já consumidos, tornando o reprocessamento seguro mesmo após falhas parciais sem risco de duplicação; 
(3) separação de responsabilidades — os consumers de ingestão escrevem apenas no Bronze, sem conhecimento das regras de qualidade da camada Silver, mantendo os dois processos independentes e substituíveis.

3.3.2 Camada Bronze

**Objetivo**

A camada Bronze é o ponto de entrada de todos os dados no Data Lakehouse e constitui a sua componente "lake" — a fonte de verdade histórica imutável do sistema. O seu objectivo é único e deliberadamente restrito: receber dados das três fontes no formato mais próximo possível do original e preservá-los sem qualquer transformação de conteúdo. A única modificação introduzida pelos consumers é a adição de dois campos de metadados de ingestão — ingested_at (timestamp de receção) e source (mecanismo de ingestão: kafka, debezium ou minio) — que permitem rastrear a proveniência de cada registo sem alterar o dado em si.
Esta imutabilidade tem uma consequência arquitectural central: em caso de erro ou alteração das regras de qualidade na camada Silver, é sempre possível reprocessar os dados originais a partir do Bronze sem necessidade de re-ingestão da fonte. A camada Bronze desacopla assim o ritmo de chegada dos dados do ritmo do seu processamento, tornando os dois processos independentes e substituíveis sem perda de informação.

**Fonte 1 — ****Clickstream**** de Navegação**

Origen
Os eventos de clickstream têm origem no módulo simulator/producer.py, que publica cada evento no tópico Kafka clickstream_events em formato JSON. O Kafka actua como buffer durável entre o simulador e o consumer de ingestão, garantindo que nenhum evento se perde mesmo que o consumer esteja temporariamente indisponível.

Extração e Transporte
Para extrair e transportar os eventos o módulo ingestion/consumer.py subscreve o tópico Kafka e consome as mensagens em modo contínuo. Cada mensagem JSON é desserializada e o campo properties — que é um objecto JSON aninhado com estrutura variável por event_type — é serializado de volta para string antes de ser escrito, preservando o dado bruto sem impor schema fixo na ingestão. Esta decisão é intencional: forçar o parsing do campo properties no Bronze introduziria um schema rígido incompatível com a variabilidade da fonte; a desserialização fica reservada para a camada Silver.

Armazenamento
Os eventos são acumulados em memória e escritos em ficheiros Parquet no sub-bucket bronze/clickstream/, com particionamento hierárquico em quatro níveis: year, month, day, hour.

Fonte 2 — Ordens de Compra (CDC)

Origem
As ordens de compra estão escritas na tabela simulated_orders do PostgreSQL.

Extracção
O Debezium monitoriza continuamente o Write-Ahead Log (WAL) do PostgreSQL e captura cada mutação da tabela — INSERT, UPDATE ou snapshot inicial — publicando-a no tópico Kafka debezium.public.simulated_orders encapsulada num envelope JSON com os campos before, after, op e ts_ms. Este mecanismo de Change Data Capture permite capturar a totalidade do ciclo de vida de cada registo sem qualquer alteração ao schema ou ao código do simulador.

Transporte
O módulo ingestion/cdc_consumer.py subscreve o tópico Debezium e desembrulha o envelope CDC para extrair o campo after, que contém o estado completo da linha após a mutação. São processadas as operações c (INSERT), u (UPDATE) e r (snapshot inicial); os eventos d (DELETE) são descartados porque o modelo de dados do simulador não produz eliminações de ordens. Os campos cdc_operation e cdc_ts_ms são preservados no registo Parquet para que a camada Silver consiga rastrear a origem e a ordem temporal de cada evento. A inclusão dos UPDATEs no Bronze é intencional: o simulador executa uma operação UPDATE sem alteração de dados (noop_update) para simular o comportamento de at-least-once delivery do Debezium, gerando um segundo evento com o mesmo order_id; se esses UPDATEs fossem descartados no Bronze, o mecanismo de deduplicação MERGE INTO da camada Silver não teria oportunidade de actuar.

Armazenamento
Os registos são escritos em ficheiros Parquet no sub-bucket bronze/orders/, com o mesmo esquema de particionamento de quatro níveis do clickstream: year, month, day, hour.

Fonte 3 — Avaliações de Clientes (File Polling)

Origem
As avaliações são escritas pelo como ficheiros de texto no bucket MinIO raw-reviews/. Cada ficheiro tem o nome {review_id}_{order_id}.txt — os únicos metadados estruturados associados à avaliação estão codificados no nome do ficheiro, não no seu conteúdo.

Extracção e transporte
O módulo ingestion/file_watcher.py inspeciona o bucket raw-reviews/ a cada 30 segundos via API S3 compatível com MinIO. Para cada ficheiro .txt ainda não processado — identificado por comparação com o conjunto de object_keys registados em .watcher_state.json — o conteúdo completo é lido e incluído como campo raw_content no registo Parquet, preservando o texto livre intacto para extracção de informação na camada Silver. Após a escrita do batch, o object_key de cada ficheiro processado é adicionado ao ficheiro de estado, garantindo que o mesmo ficheiro nunca é ingerido duas vezes mesmo após reinicialização do processo.

Armazenamento
As avaliações são escritas em ficheiros Parquet no sub-bucket bronze/reviews/, com particionamento em três níveis: year, month, day — um nível a menos do que as outras duas fontes, dado o volume mais reduzido e a ausência de granularidade horária no mecanismo de polling.

**Métodos Comuns**

Particionamento hierárquico no padrão Hive
O particionamento é o mecanismo pelo qual os ficheiros Parquet são organizados em subpastas hierárquicas dentro de cada sub-bucket, seguindo a convenção campo=valor reconhecida nativamente pelo Spark e pelo PyArrow. O seu objectivo é a eliminação de partições na leitura: quando o job Silver processa apenas os dados de uma determinada hora, o Spark filtra a query pela partição correspondente e ignora completamente todas as outras pastas — evitando a leitura e desserialização de ficheiros irrelevantes, o que reduz significativamente o volume de I/O e o tempo de execução em proporção directa com a selectividade temporal do filtro.
O clickstream e as ordens utilizam quatro níveis de partição (year, month, day, hour) porque são gerados em modo contínuo com volume horário expressivo, tornando a granularidade horária relevante para a eliminação de partições. As avaliações utilizam três níveis (year, month, day) porque o seu volume mais reduzido e a cadência de polling de 30 segundos não justificam subdivisão horária. Cada ficheiro dentro da partição é nomeado batch_<unix_timestamp>.parquet, onde o timestamp unix identifica univocamente o momento de escrita:
bronze/clickstream/year=2026/month=05/day=25/hour=08/batch_1748174400.parquet
 bronze/orders/year=2026/month=05/day=25/hour=08/batch_1748174400.parquet
 bronze/reviews/year=2026/month=05/day=25/batch_1748174400.parquet

Micro-batching

O micro-batching é a estratégia pela qual os consumers acumulam registos em memória antes de os escrever num único ficheiro Parquet, em vez de escreverem um ficheiro por registo. O seu objectivo é equilibrar dois requisitos conflituantes: baixa latência de disponibilização dos dados no Bronze e eficiência de leitura pelo Spark. Ficheiros demasiado pequenos — um por mensagem Kafka, por exemplo — degradam o desempenho de leitura do Spark por excesso de overhead de metadata do sistema de ficheiros; ficheiros demasiado grandes aumentam o tempo que os dados aguardam em memória antes de ficarem disponíveis no Bronze.
Os consumers Kafka (consumer.py) e CDC (cdc_consumer.py) implementam este equilíbrio com dois critérios de flush alternativos: o buffer é escrito quando atinge 500 registos ou quando decorrem 30 segundos desde o último flush, o que ocorrer primeiro. Este duplo critério garante que em períodos de tráfego elevado os ficheiros têm dimensão razoável, e que em períodos de tráfego reduzido os dados não ficam retidos em memória por mais de 30 segundos. O file watcher (file_watcher.py) não mantém buffer contínuo entre ciclos — escreve um único ficheiro Parquet com todos os ficheiros .txt novos encontrados em cada ciclo de polling de 30 segundos, o que é equivalente a um flush imediato por batch.

3.3.3 Camada Silver

**Objectivo**

A camada Silver é a componente "house" do Data Lakehouse — o ponto onde os dados deixam de ser armazenamento bruto e passam a ser uma base analítica estruturada. O seu objectivo é transformar os ficheiros Parquet do Bronze em tabelas Apache Iceberg limpas, tipadas, semanticamente enriquecidas e com garantias ACID, prontas a ser consumidas pela equipa analytical_engineering para a construção do modelo dimensional Gold. É nesta camada que se concentra toda a lógica de qualidade de dados do sistema: a camada Bronze apenas preserva; a camada Silver garante.
Cada um dos três jobs Spark de transformação Silver lê os ficheiros Parquet do Bronze correspondente, aplica regras de validação e enriquecimento específicas ao tipo de dado, e escreve o resultado numa tabela Iceberg registada no catálogo lake através do Hive Metastore.

**Job 1 — silver_clickstream.py**

Entrada
Ficheiros Parquet de bronze/clickstream/, lidos com schema explícito para evitar inferência automática.

Transformações e regras de qualidade
O campo properties — preservado como string JSON no Bronze — é desserializado para struct com schema definido, extraindo os atributos específicos de cada event_type (product_id, category, price, etc.). Os eventos com session_id nulo ou vazio são descartados, pois sem identificador de sessão não é possível reconstruir qualquer sequência de navegação. Os eventos sem event_type válido ou sem event_ts são igualmente descartados. Os valores do campo device são normalizados para o vocabulário controlado {mobile, desktop, tablet}: valores não reconhecidos — como "bot" ou "crawler", injectados pelo simulador como ruído — são normalizados para "unknown". Por fim, filtra-se para os 11 tipos de evento válidos do funil de conversão.

Saída
Tabela Iceberg lake.silver.clickstream com schema fixo e tipagem forte, em modo append.

**Job 2 — silver_orders.py**

Entrada
Ficheiros Parquet de bronze/orders/, que contêm eventos CDC do Debezium com os campos operacionais da tabela simulated_orders e os metadados cdc_operation e cdc_ts_ms.

Transformações e regras de qualidade
Os campos price e freight_value são convertidos de string para DOUBLE — o Debezium serializa colunas NUMERIC do PostgreSQL como string binária no Parquet — e os registos com price ≤ 0 ou freight_value < 0 são descartados como inválidos. O campo state é validado contra o conjunto dos 27 códigos de estado brasileiros: estados não reconhecidos (como "sao paulo") têm state definido como NULL e region como "Desconhecido", sem descartar a ordem. O campo total_value é calculado como round(price + freight_value, 2). Cada ordem é enriquecida com o campo region, derivado do mapeamento dos 27 estados para as cinco macrorregiões brasileiras.

Saída
Tabela Iceberg lake.silver.orders, com deduplicação garantida por dois níveis (detalhados na secção Mecanismos Comuns).

**Job 3 — silver_reviews.py**

Entrada
Ficheiros Parquet de bronze/reviews/, que contêm o campo raw_content com o texto integral de cada avaliação e o file_path com o nome do ficheiro original.

Transformações e regras de qualidade
O review_id e o order_id são extraídos do nome do ficheiro ({review_id}_{order_id}.txt) por partição no carácter _; registos cujo nome de ficheiro não permita extrair ambos os identificadores são descartados. O rating é extraído do texto livre por duas expressões regulares correspondentes às duas formas produzidas pelo simulador — "Dou X estrelas em 5" e "Classifico este produto com X/5" —; registos para os quais nenhum padrão produz um valor válido no intervalo [1, 5] são descartados. O text_length é calculado como contagem de palavras por split em espaços. O raw_content completo é preservado como campo message para consumo posterior.

Saída
Tabela Iceberg lake.silver.reviews, com deduplicação garantida por MERGE INTO (detalhado na secção Mecanismos Comuns).

**Mecanismos Comuns**

Checkpoint do Spark Structured Streaming

Os três jobs utilizam trigger(availableNow=True), que instrui o Spark a processar todos os ficheiros Bronze ainda não consumidos e a terminar — em vez de correr indefinidamente como um stream contínuo. O estado de progresso é mantido num checkpoint persistido em MinIO (s3a://silver/_checkpoints/{clickstream,orders,reviews}): o Spark regista neste checkpoint os ficheiros Bronze já processados em runs anteriores e, no início de cada novo run, lê apenas os ficheiros ainda não registados. Este mecanismo garante idempotência: se um job falhar a meio e for relançado pela DAG, não reprocessa dados já escritos na Silver.

Deduplicação com MERGE INTO Iceberg

Os jobs de ordens e reviews enfrentam um problema adicional de deduplicação: o Bronze pode conter múltiplos eventos para o mesmo order_id (devido ao at-least-once delivery do Debezium) ou para o mesmo review_id (devido às submissões duplicadas injectadas pelo simulador). A deduplicação é feita em dois níveis. Dentro de cada micro-batch processado pelo Spark, uma window function (ROW_NUMBER OVER PARTITION BY order_id/review_id ORDER BY cdc_ts_ms DESC) retém apenas o evento mais recente por chave. Entre runs, o MERGE INTO Iceberg com WHEN NOT MATCHED THEN INSERT * garante que um order_id ou review_id já existente na Silver nunca é duplicado por runs posteriores — independentemente do número de vezes que o mesmo evento apareça no Bronze. O job de clickstream não necessita deste mecanismo porque os eventos de clickstream são naturalmente únicos por event_id e são escritos em modo append directo.

O código dos três jobs é da responsabilidade da equipa data_engineering; a execução é agendada e despoletada pela DAG trendmart_gold_pipeline da equipa infrastructure (infrastructure/dags/dag_trendmart.py), com cadência horária definida como requisito de SLA pela equipa analytical_engineering.

3.4 Área de Dados do Sistema de Análise
A secção 3.4 descreve a camada de dados que suporta directamente a equipa data_analytics — a equipa técnica responsável pelo dashboard interativo que constitui o ponto de consumo final do pipeline TrendMart.

Conforme definido no capítulo 2, a equipa data_analytics não serve um único tipo de utilizador: o produto que entrega ramifica-se em seis abas, cada uma orientada a um perfil de utilizador distinto com perguntas de negócio próprias e uma janela temporal específica. Os três perfis de natureza ERP — Gestor Executivo, Analista de Vendas e Analista de Tendências — consomem dados transacionais e operacionais: receita, volume de encomendas, decomposição por categoria e região, crescimento e anomalias. Os três perfis de natureza CRM — Analista de Marketing, Gestor de Customer Experience e Data Scientist — consomem dados comportamentais e relacionais: funil de conversão, sentimento de cliente e previsão de procura. Esta ramificação — uma única equipa técnica, seis perfis de consumo — é o que define os requisitos funcionais atribuídos à equipa no capítulo 2: RF1.1 a RF1.4 (Gestor Executivo), RF2.1 a RF2.4 (Analista de Vendas), RF3.1 a RF3.4 (Analista de Marketing), RF4.1 a RF4.4 (Gestor de Customer Experience), RF5.1 a RF5.4 (Analista de Tendências) e RF6.1 a RF6.2 (Data Scientist) — 22 requisitos funcionais no total.

O objectivo desta camada de dados é satisfazer esses 22 requisitos sem que o dashboard precise de construir joins, agregar dados ou aplicar lógica de negócio. Toda a complexidade de agregação, resolução de chaves estrangeiras e cálculo de métricas derivadas — crescimento semana-sobre-semana, aceleração da procura, classificação de sentimento, detecção de anomalias — fica encapsulada no modelo dimensional Gold e nas seis vistas Trino que o expõem, tornando o dashboard um consumidor directo de dados prontos para visualização.

Esta secção organiza-se em três partes: o modelo dimensional que estrutura os dados Gold (3.4.1), a sua implementação física em Apache Iceberg (3.4.2), e as vistas Trino que expõem o modelo ao dashboard (3.4.3).

3.4.1 Modelo Dimensional — Escolhas de Design

O sistema de dados analítico do TrendMart implementa um modelo dimensional em esquema estrela na camada Gold do Data Lake, armazenado em tabelas Apache Iceberg sobre MinIO e acessível via Trino. A escolha de um modelo dimensional — em detrimento de uma modelação relacional normalizada (3NF) — decorre dos padrões de acesso analítico dominantes: queries que atravessam múltiplas dimensões (por exemplo, "receita por categoria, por mês, por região"), contagens e agregações sobre grandes volumes de factos, e filtragens sobre atributos descritivos das dimensões. O esquema estrela minimiza o número de joins necessários por query analítica e torna as estruturas de dados imediatamente compreensíveis para analistas sem conhecimento técnico do pipeline. O processo de conceção seguiu os quatro passos do método de Kimball, com as escolhas de design ancoradas nos requisitos funcionais do capítulo 2.

**Passo 1 — Identificação dos Processos de Negócio**

Foram identificados três processos de negócio distintos mas complementares, cada um correspondendo a uma tabela de factos:
(i) Vendas e Encomendas: o ciclo de vida de uma encomenda desde a compra até à sua conclusão, com métricas financeiras — suporta os perfis Gestor Executivo (RF1.1–RF1.4), Analista de Vendas (RF2.1–RF2.4) e Analista de Tendências (RF5.1–RF5.4);
(ii) Comportamento de Navegação: os eventos que os utilizadores geram na plataforma, que permitem analisar o funil de conversão digital — suporta o perfil Analista de Marketing (RF3.1–RF3.4);
(iii) Avaliações de Clientes: o feedback qualitativo pós-compra, com classificação de sentimento — suporta o perfil Gestor de Customer Experience (RF4.1–RF4.4).

**Passo 2 — Definição da Granularidade**

Para fact_sales, a granularidade é ao nível da ordem individual: cada linha representa uma transação de compra. Esta granularidade permite agregar métricas em qualquer nível superior (categoria, dia, região) sem perda de informação.
Para fact_clickstream, a granularidade é ao nível do evento individual: cada linha representa um clique, visualização, ou acção no funil. Esta granularidade permite reconstruir sessões completas e calcular taxas de conversão em qualquer ponto do funil.
Para fact_reviews, a granularidade é ao nível da avaliação individual: cada linha representa um ficheiro de review processado, com o rating, sentimento, categoria e região associados.

**Passo 3 — Identificação das Dimensões**

A seleção das dimensões partiu da análise dos eixos de análise exigidos pelos requisitos funcionais do capítulo 2. Todos os perfis necessitam de análise temporal; os perfis de natureza ERP (Analista de Vendas, Analista de Tendências) e o Gestor de Customer Experience necessitam de análise por categoria; o Analista de Vendas e o Gestor de Customer Experience necessitam de análise geográfica. Isto determina exactamente três dimensões, necessárias e suficientes:
dim_date: responde ao quando. Criada como calendário completo de 2020 a 2030 (3 652 datas), com atributos derivados: year, month, quarter, week, day_of_week, is_weekend. É partilhada pelas três tabelas de factos e pelas tabelas ML.
dim_category: responde ao o quê ao nível de categoria. Contém os nomes de categoria em português e inglês derivados das ordens e do clickstream, com category_id gerado por hash determinístico do nome. Permite análise por categoria em qualquer tabela de factos.
dim_geography: responde ao onde. Mapeia os 27 estados brasileiros para as cinco macrorregiões (Norte, Nordeste, Centro-Oeste, Sudeste, Sul), com geo_id gerado por hash do código de estado.
Não foram criadas dimensões ao nível de produto ou de cliente porque nenhum requisito funcional do capítulo 2 exige análise a esse nível de detalhe — toda a análise de produto é feita ao nível de categoria (RF2.1, RF4.3, RF5.4) e toda a análise de cliente é expressa em contagens de clientes únicos por dia (RF1.1, RF2.2), não em atributos de clientes individuais.

**Passo 4 — Identificação dos Factos**

fact_sales contém métricas aditivas (price, freight_value, total_value), as chaves de negócio order_id, customer_id e session_id, e chaves estrangeiras para as três dimensões (date_id, category_id, geo_id). O customer_id é preservado como atributo degenerado para permitir a contagem de clientes únicos (RF1.1, RF2.2) sem necessidade de uma dimensão dim_customer.
fact_clickstream contém event_type e device como factos degenerados (vocabulários controlados com número fixo de valores), session_id e user_id como chaves de negócio, e chaves estrangeiras para dim_date (date_id) e dim_category (category_id). Não tem FK para dim_geography porque nenhum requisito do perfil Analista de Marketing (RF3.1–RF3.4) exige análise geográfica do comportamento de navegação.
fact_reviews contém rating (semi-aditivo — o indicador relevante é a média, não a soma), sentiment como facto degenerado (positive, neutral, negative, classificado em gold_reviews.py), e chaves estrangeiras para as três dimensões (date_id, category_id, geo_id). O geo_id é resolvido em gold_reviews.py por join com silver.orders para obter o estado do cliente da ordem associada, seguido de lookup em dim_geography — garantindo que a geografia fica inscrita no próprio facto e não depende de um join posterior através de fact_sales, o que causaria NULLs em reviews cujas ordens ainda não tivessem chegado ao Gold no momento da execução da DAG.
A tabela ML ml_demand_forecast é uma tabela Gold adicional que não faz parte do modelo dimensional principal mas partilha a convenção de nomenclatura e é acessível via Trino. É gerada pela DAG independente trendmart_ml_pipeline e consumida directamente pelo dashboard sem passar por vista.

3.4.2 Implementação Física do Schema

Os jobs Spark da equipa analytical_engineering são invocados pela DAG trendmart_gold_pipeline com cadência horária. Existem dois padrões de escrita distintos consoante o tipo de tabela.

**Dimensões — ****createOrReplace**** a cada ****run**

Os três jobs de dimensões (gold_dimensions.py) recriam integralmente dim_date, dim_category e dim_geography a cada execução via writeTo().createOrReplace(). Esta escolha é deliberada: as dimensões têm um conjunto de valores pequeno e estável, e recalculá-las completamente garante que qualquer nova categoria ou novo estado presente no Silver fica reflectido imediatamente, sem lógica de merge ou detecção de diferenças.

**Tabelas de factos — ****append**** incremental com janela ****Airflow**

Os três jobs de factos (gold_sales.py, gold_clickstream.py, gold_reviews.py) usam um modelo de carga incremental baseado na janela de execução do Airflow. O DAG passa a cada job os parâmetros data_interval_start e data_interval_end — os timestamps exatos da hora que está a ser processada —, e o job filtra a tabela Silver pelo campo ingested_at dentro dessa janela:
ingested_at >= data_interval_start  AND  ingested_at < data_interval_end
O campo ingested_at é atribuído por-registo no momento em que cada consumer Bronze recebe a mensagem (do Kafka, do CDC ou do MinIO), pelo que reflecte o instante real de ingestão de cada evento individual — não o timestamp do ficheiro Parquet que o contém.
Na primeira execução, quando a tabela Gold ainda não existe, o job usa writeTo().createOrReplace() para a criar com o schema e particionamento corretos. Nas execuções seguintes usa writeTo().append(), adicionando apenas os registos da hora processada. Se não existirem registos na janela — por exemplo num período de tráfego nulo —, o job termina sem escrever, evitando commits vazios.
Este modelo é distinto do mecanismo incremental da camada Silver: os jobs Silver usam Spark Structured Streaming com trigger(availableNow=True) e um checkpoint persistido em MinIO, que rastreia quais ficheiros Bronze já foram lidos pelo path — sem dependência do Airflow. O Gold delega essa responsabilidade ao orquestrador, o que torna cada run Gold determinístico e reproduzível: re-executar a janela das 13:00–14:00 produz sempre o mesmo resultado, independentemente de quando a re-execução ocorre.

**Tabelas ML — ****createOrReplace**** diário**

A tabela ml_demand_forecast é substituída na totalidade a cada execução diária da DAG trendmart_ml_pipeline via createOrReplace, porque o modelo é retreinado de raiz com o histórico completo disponível nesse momento.

**Catálogo partilhado**

O catálogo Iceberg é gerido pelo Hive Metastore (protocolo Thrift), partilhado entre o Spark (escrita) e o Trino (leitura). Esta partilha permite ao Trino ver imediatamente as tabelas escritas pelo Spark sem registo manual — é o que torna as tabelas Gold consultáveis pelo dashboard logo após cada execução da DAG.

3.4.3 Camada Analítica: Vistas Trino

Sobre o schema físico Gold são criadas seis vistas SQL pelo script analytical_engineering/views/init_views.py, executado como última task da DAG Airflow após a conclusão dos jobs Gold. As vistas materializam os joins mais frequentes entre factos e dimensões e expõem os dados em formato directamente consumível pelo dashboard Dash sem necessidade de construir joins no cliente. Cada vista corresponde a um perfil de utilizador definido no capítulo 2, conforme descrito abaixo.
vw_executive — Perfil: Gestor Executivo (RF1.1–RF1.4): agrega KPIs diários globais — receita total, número de ordens, número de clientes únicos (COUNT DISTINCT customer_id), valor médio por ordem, rating médio e contagens de reviews por sentimento. Responde directamente aos requisitos RF1.1 (KPIs globais — receita, encomendas, clientes únicos e valor médio), RF1.2 (evolução diária da receita), RF1.3 (volume diário de reviews positivas e negativas) e RF1.4 (rating médio diário). Consumida pela aba Executivo do dashboard.
vw_sales_performance — Perfil: Analista de Vendas (RF2.1–RF2.4): detalha as vendas por data, categoria, estado e macrorregião, incluindo decomposição de receita em produto e frete. Responde a RF2.1 (top 10 categorias por receita), RF2.2 (distribuição de receita por macrorregião e estado), RF2.3 (evolução temporal da receita com granularidade diária) e RF2.4 (decomposição de receita em valor de produto e frete). Consumida pela aba Vendas do dashboard.
vw_funnel — Perfil: Analista de Marketing (RF3.1–RF3.4): agrega os eventos de clickstream por hora, tipo de evento, dispositivo e categoria, expondo o funil de conversão. O evento order_placed não existe no clickstream — os utilizadores que chegam ao checkout transitam directamente para a fonte de ordens. Por isso, a vista une via UNION ALL os eventos de clickstream com uma subquery sobre fact_sales, que contribui com um registo sintético order_placed por ordem. Responde a RF3.1 (funil completo de eventos com contagens por etapa), RF3.2 (desagregação por tipo de dispositivo), RF3.3 (evolução diária dos top 5 eventos numa janela de 7 dias) e RF3.4 (número de sessões e utilizadores únicos por dia). Consumida pela aba Funil do dashboard.
vw_reviews — Perfil: Gestor de Customer Experience (RF4.1–RF4.4): agrega as avaliações por data, sentimento, categoria e região, com contagens e rating médio. Os joins de categoria e geografia são feitos directamente sobre os campos category_id e geo_id de fact_reviews — o geo_id é resolvido em gold_reviews.py por join com silver.orders, pelo que a vista não necessita de atravessar fact_sales para obter a geografia. Responde a RF4.1 (distribuição de reviews por sentimento — positivo, neutro e negativo), RF4.2 (evolução do rating médio diário numa janela de 90 dias), RF4.3 (top 10 categorias por volume de reviews com rating médio) e RF4.4 (distribuição de sentimento por macrorregião geográfica). Consumida pela aba Reviews do dashboard.
vw_trends — Perfil: Analista de Tendências (RF5.1–RF5.4): calcula métricas de tendência globais por dia — taxa de crescimento week-over-week de ordens e receita, aceleração da procura, e flag de anomalia com base num desvio superior a 2σ da média calculada sobre uma janela histórica de 60 dias (ROWS BETWEEN 59 PRECEDING AND CURRENT ROW). Responde a RF5.1 (crescimento WoW da receita e encomendas), RF5.2 (aceleração da procura — variação da taxa de crescimento entre semanas), RF5.3 (detecção e sinalização de anomalias numa janela de 60 dias) e RF5.4 (crescimento médio por categoria via vw_category_trends). Consumida pela aba Tendências do dashboard.
vw_category_trends — Perfil: Analista de Tendências (RF5.4): detalha as métricas de crescimento week-over-week por categoria e semana, permitindo identificar categorias com procura em expansão ou contracção. Complementa vw_trends ao nível de detalhe por categoria exigido por RF5.4. Consumida pela aba Tendências do dashboard em modo de detalhe por categoria.
O perfil Data Scientist (RF6.1–RF6.2) não tem vista dedicada: os seus requisitos são satisfeitos pelo consumo directo da tabela lake.gold.ml_demand_forecast, gerada pela DAG trendmart_ml_pipeline, sem necessidade de agregação intermédia.

3.5 O Processo de Integração de Dados
3.5.1 Método e Ferramentas
O processo de integração de dados é da responsabilidade principal da equipa data_engineering, que implementa as duas primeiras fases do pipeline — ingestão Bronze e transformação Silver —, com suporte da equipa infrastructure na orquestração Airflow e da equipa analytical_engineering na carga Gold, descrita em detalhe na secção 3.4.
O paradigma adoptado é ELT (Extract, Load, Transform), em oposição ao ETL tradicional. Nas três fontes contínuas (clickstream, ordens CDC e reviews), os dados são primeiro extraídos e carregados no Data Lake Bronze no seu formato original — sem qualquer transformação —, e só depois transformados nas camadas Silver e Gold. Esta escolha justifica-se por três razões: preserva os dados originais indefinidamente na camada Bronze como fonte de verdade imutável; desacopla o ritmo de ingestão (contínuo, controlado pelos consumers) do ritmo de transformação (horário, controlado pelo Airflow); e permite reprocessar toda a cadeia Silver → Gold a partir dos dados Bronze sem necessitar de re-ingestão das fontes. Um paradigma ETL clássico eliminaria esta possibilidade ao transformar os dados antes do armazenamento.
Para modelar e documentar o processo de integração utiliza-se a notação BPMN 2.0 (Business Process Model and Notation), que permite representar os fluxos de dados, os agentes responsáveis por cada fase e as dependências entre tarefas numa notação padronizada e independente da implementação. O modelo BPMN do sistema é apresentado na secção 3.5.3 e o diagrama completo consta do Anexo A.
As ferramentas utilizadas no processo de integração — Apache Kafka, Debezium, Apache Spark, Apache Iceberg, Apache Airflow e MinIO — foram caracterizadas e justificadas na secção 3.1.3. No contexto deste processo, cada ferramenta ocupa um papel específico: o Kafka e o Debezium transportam os eventos das fontes até aos consumers Bronze; o MinIO persiste os ficheiros Parquet Bronze e os checkpoints Silver; o Spark executa as transformações Bronze → Silver e Silver → Gold; o Iceberg garante as propriedades ACID necessárias para MERGE INTO (deduplicação Silver) e append incremental (carga Gold); e o Airflow orquestra a sequência completa com dependências explícitas, retentativas automáticas e registo auditável de cada execução.

3.5.2 Mapeamento de Dados Source-to-Target
O mapeamento source-to-target descreve as correspondências entre os campos das fontes de dados e as colunas das tabelas finais do modelo analítico Gold, bem como todas as transformações aplicadas em cada fase do processo de integração. Para cada fonte, o mapeamento é apresentado em dois sub-passos: Bronze → Silver (qualidade, tipagem e deduplicação) e Silver → Gold (resolução de chaves estrangeiras dimensionais).

**Clickstream (Bronze → Silver: lake.silver.clickstream)**

| Campo Fonte (Bronze) | Transformação | Campo Destino (Silver) |
| --- | --- | --- |
| event_id | Descartado se NULL; checkpoint de streaming garante não-releitura de ficheiros | event_id |
| session_id | Descartado se NULL ou vazio | session_id |
| user_id | Cópia directa | user_id |
| event_type | Descartado se NULL ou vazio | event_type |
| timestamp | to_timestamp() | event_ts |
| device | Normalização: {mobile, desktop, tablet} → cópia directa; outros valores → "unknown" | device |
| properties (JSON) | from_json(): extracção de category, product_id, price | category, product_id, price |
| properties.state / properties.city | coalesce(props.state, props.city) | location |
| ingested_at | to_timestamp() | ingested_at |

**Clickstream (Silver → Gold: lake.gold.fact_clickstream)**

| Campo Fonte (Silver) | Transformação | Campo Destino (Gold) |
| --- | --- | --- |
| event_ts | to_date() | event_date |
| event_date | Join a dim_date por date_actual | date_id (FK, nullable) |
| category | Join a dim_category por category_en | category_id (FK, nullable) |
| event_id, session_id, user_id, event_type, event_ts, device, product_id, price, location | Cópia directa | (mesmos nomes) |

**Orders (Bronze → Silver: lake.silver.orders)**

| Campo Fonte (Bronze) | Transformação | Campo Destino (Silver) |
| --- | --- | --- |
| order_id | MERGE INTO por order_id; dentro do batch mantém o registo com maior cdc_ts_ms | order_id |
| session_id, customer_id, product_id, seller_id | Descartado se customer_id NULL | session_id, customer_id, product_id, seller_id |
| category | trim() | category |
| price (STRING) | cast(DOUBLE); descartado se NULL ou ≤ 0 | price |
| freight_value (STRING) | cast(DOUBLE); descartado se NULL ou < 0 | freight_value |
| price + freight_value | round(price + freight_value, 2) | total_value |
| purchase_timestamp (LONG µs) | (/ 1 000 000).cast(TIMESTAMP); descartado se não parseável | purchase_ts |
| state | upper(trim()); validação contra 27 códigos brasileiros → NULL se inválido | state |
| state (validado) | Join a tabela auxiliar REGION_MAP | region |
| ingested_at | to_timestamp() | ingested_at |

**Orders (Silver → Gold: lake.gold.fact_sales)**

| Campo Fonte (Silver) | Transformação | Campo Destino (Gold) |
| --- | --- | --- |
| purchase_ts | to_date() | purchase_date |
| purchase_date | Join a dim_date por date_actual | date_id (FK, nullable) |
| category | Join a dim_category por category_en | category_id (FK, nullable) |
| state | Join a dim_geography por state | geo_id (FK, nullable) |
| order_id, session_id, customer_id, product_id, seller_id, price, freight_value, total_value, purchase_ts | Cópia directa | (mesmos nomes) |

**Reviews (Bronze → Silver: lake.silver.reviews)**

| Campo Fonte (Bronze) | Transformação | Campo Destino (Silver) |
| --- | --- | --- |
| review_id | MERGE INTO por review_id; descartado se NULL ou vazio | review_id |
| order_id | Descartado se NULL ou vazio | order_id |
| raw_content (texto livre) | regexp_extract: "Dou N estrelas em 5" ou "Classifico com N/5" | rating (INT) |
| rating | Validação intervalo [1, 5]; descartado fora do intervalo | rating |
| raw_content | trim() | message |
| message | size(split(message, \s+)) — contagem de palavras | text_length |
| ingested_at | to_timestamp() | ingested_at |

**Reviews (Silver → Gold: lake.gold.fact_reviews)**

| Campo Fonte (Silver) | Transformação | Campo Destino (Gold) |
| --- | --- | --- |
| ingested_at | to_date() | review_date |
| review_date | Join a dim_date por date_actual | date_id (FK, nullable) |
| order_id | Join a silver.orders para obter order_category e order_state | (campos intermediários) |
| order_category | Join a dim_category por category_en | category_id (FK, nullable) |
| order_state | Join a dim_geography por state | geo_id (FK, nullable) |
| rating | CASE WHEN ≥ 4 → "positive", = 3 → "neutral", ≤ 2 → "negative" | sentiment |
| review_id, order_id, rating, message, text_length | Cópia directa | (mesmos nomes) |

3.5.3 Fluxo do Processo de Integração (BPMN)
O processo de integração de dados é modelado segundo a notação BPMN 2.0, com três pools sequenciais correspondentes às três camadas do Data Lake: o **Pool Bronze**, responsável pela ingestão contínua das fontes para o MinIO Bronze; o **Pool Silver**, responsável pela transformação e validação Bronze → Silver via Spark; e o **Pool Gold**, responsável pela integração no modelo dimensional Gold e criação das vistas Trino. A orquestração entre pools Silver e Gold é da responsabilidade do Apache Airflow. O diagrama BPMN completo encontra-se no Anexo A deste relatório.
**Pool Bronze — Ingestão Contínua (3 ****consumers**** paralelos)**
Os três consumers de ingestão correm em paralelo e de forma contínua, independentemente da DAG Airflow. O consumer Kafka (consumer.py) subscreve o tópico clickstream_events e bufferiza os eventos até ao limiar de 500 registos ou 30 segundos, escrevendo então um ficheiro Parquet na partição Bronze correspondente ao dia atual. O consumer CDC (cdc_consumer.py) subscreve o tópico Debezium e aplica o mesmo mecanismo de bufferização, extraindo apenas os campos relevantes do envelope CDC antes de escrever no Bronze. O file watcher (file_watcher.py) consulta o bucket raw-reviews/ a cada 30 segundos, identifica novos ficheiros por comparação com o estado persistido em .watcher_state.json, e escreve os conteúdos em Parquet Bronze.
**Pool Silver — DAG Airflow (task `silver_*`)**
A DAG Airflow desencadeia os três jobs Spark Silver em paralelo. Ao contrário do Gold, os jobs Silver não operam sobre um intervalo de tempo fixo — usam Spark Structured Streaming com trigger(availableNow=True) e um checkpoint persistido no MinIO que regista quais ficheiros Bronze já foram processados. A cada execução, cada job lê todos os ficheiros Bronze novos desde o último run, aplica as transformações e regras de qualidade de dados descritas na secção 3.3, e escreve nas tabelas Iceberg Silver correspondentes usando MERGE INTO para as fontes com risco de duplicação (ordens e reviews) e append directo para o clickstream.
**Pool Gold — DAG ****Airflow**** (****tasks**** `****gold****_*` e `****init_views****`)**
Após a conclusão das tasks Silver, a DAG executa os jobs Gold em sequência determinística: gold_dimensions primeiro (que recria as três dimensões com createOrReplace), depois gold_sales, gold_clickstream e gold_reviews em paralelo (que fazem append incremental às tabelas de factos), e finalmente init_views que actualiza as seis vistas Trino. Esta ordem garante que as dimensões estão disponíveis antes de qualquer job de factos tentar resolver chaves estrangeiras.
3.5.4 Orquestração com Apache Airflow
O sistema é orquestrado por duas DAGs Airflow independentes com cadências distintas.
A **DAG `trendmart_gold_pipeline`** corre de hora a hora (0 * * * *), com max_active_runs=1 para garantir que os batches são sempre processados em série — nunca dois runs em paralelo — o que é necessário porque os jobs Spark partilham o catálogo Iceberg e os buckets MinIO. O grafo de dependências da DAG é:
silver_clickstream ──┐
silver_orders      ──┼──► gold_dimensions ──┬──► gold_clickstream ──┐
silver_reviews     ──┘                      ├──► gold_sales       ──┼──► init_views
                                            └──► gold_reviews     ──┘
As tasks Silver correm em paralelo entre si (sem dependências mútuas); as tasks Gold de factos correm em paralelo após gold_dimensions completar; init_views aguarda a conclusão de todas as tasks Gold. Em caso de falha, o Airflow retenta cada task automaticamente: as tasks Silver com um atraso de 15 minutos (necessário para a JVM Spark do run anterior libertar completamente os recursos), as tasks Gold com um atraso de 5 minutos.
A **DAG `trendmart_ml_pipeline`** corre diariamente às 03:00 UTC, após o pipeline Gold ter os dados do dia completos. Esta DAG executa a task demand_forecast, que treina o modelo sobre as tabelas Gold e escreve as previsões na tabela lake.gold.ml_demand_forecast com createOrReplace. Em caso de falha, retenta após 10 minutos.
3.5.5 Validação e Testes
A validação do processo de integração opera em duas dimensões complementares: validação inline durante o processamento Silver, que actua como barreira de qualidade antes de qualquer dado atingir o modelo analítico, e verificação de consistência sobre o modelo Gold após a carga, baseada em contagens e garantias transacionais.

**Validação Inline na Camada Silver**

A principal barreira de qualidade de dados do sistema encontra-se na camada Silver. Cada job Spark aplica regras de validação explícitas durante a transformação, antes de qualquer dado chegar ao Gold. As regras cobrem quatro categorias de anomalias:
Integridade de identidade: registos sem event_id, session_id ou event_type (no clickstream), sem order_id ou customer_id (nas ordens), ou sem review_id ou order_id (nas reviews) são descartados antes de qualquer outra transformação.
Conformidade de vocabulários controlados: o campo state é validado contra o conjunto dos 27 códigos de estado brasileiros — registos com estado desconhecido ou malformado recebem NULL no campo state e "Desconhecido" no campo region, em vez de serem descartados, preservando o registo mas sinalizando a ausência de informação geográfica.
Invariantes de negócio: nas ordens, registos com price nulo ou negativo e com freight_value nulo ou negativo são descartados; o rating nas reviews é validado no intervalo [1, 5] e registos fora deste intervalo são removidos.
Coerência temporal: registos cujo timestamp não é parseável para TIMESTAMP são descartados no final do pipeline de transformação de cada job Silver.
A operação MERGE INTO Iceberg nas tabelas silver.orders e silver.reviews funciona como barreira adicional de deduplicação: mesmo que o Bronze contenha registos duplicados — introduzidos pelo simulador como ruído intencional ou pela semântica at-least-once do Kafka —, o Silver garante que cada order_id e cada review_id existe no máximo uma vez.

**Verificação de Consistência no Gold**

Após cada execução da DAG, a consistência do modelo Gold é verificada através de três mecanismos. Primeiro, as garantias ACID do Apache Iceberg asseguram que cada operação de escrita (append ou createOrReplace) é atómica — em caso de falha parcial do job Spark, a tabela permanece no estado anterior sem dados corrompidos ou commits parciais. Segundo, cada job de factos Gold devolve a contagem de registos processados via Airflow XCom; se a contagem for zero numa janela com actividade esperada, o Airflow regista o facto no log da task, permitindo identificar lacunas de dados sem acesso directo às tabelas. Terceiro, o módulo analytical_engineering/reporter.py gera após cada run um relatório Markdown em analytical_engineering/relatórios/, documentando o número de registos por tabela, o intervalo temporal coberto e o tempo de execução de cada task — permitindo monitorizar tendências de volume ao longo do tempo e detectar anomalias de processamento (quedas abruptas de volume, runs com duração anómala) sem necessidade de acesso directo ao Airflow ou às tabelas Gold.

**4. Exploração e Análise de Dados**
4.1 Organização Geral do Sistema de Dashboarding
A camada de visualização do TrendMart é implementada como uma aplicação web em **Plotly**** ****Dash** com Bootstrap, organizada em seis abas temáticas, cada uma concebida para responder às perguntas de negócio de um ou mais perfis de analista caracterizados no capítulo 2. A aplicação está acessível em http://localhost:8050 e actualiza automaticamente os seus dados a cada cinco minutos via um componente dcc.Interval, garantindo que a informação apresentada reflecte o estado do pipeline Gold sem necessidade de intervenção manual do utilizador.
4.1.1 Arquitectura da Camada de Visualização
A aplicação é composta por dois módulos Python distintos com responsabilidades claramente separadas.
O módulo data.py constitui a **camada de dados do ****dashboard**: contém exclusivamente funções que constroem queries SQL parametrizadas, as executam sobre as vistas Trino via o cliente trino.dbapi, e devolvem DataFrames Pandas. Cada função corresponde a uma vista analítica — get_executive, get_sales_performance, get_funnel, get_reviews, get_trends, get_category_trends, get_demand_forecast, get_churn_scores — e recebe como parâmetro a janela temporal desejada em dias. Esta separação garante que a lógica de acesso a dados pode ser testada, substituída ou extendida sem alterar a camada de apresentação.
O módulo app.py constitui a **camada de apresentação**: define o layout da aplicação (cabeçalho, tabs, área de conteúdo), regista o callback principal que responde a mudanças de aba e ao tick de refresh, e delega a construção de cada aba para funções de renderização especializadas (_tab_executive, _tab_sales, _tab_funnel, _tab_reviews, _tab_trends, _tab_ml). A arquitectura de callback único — um único @app.callback que re-renderiza o conteúdo completo da aba activa a cada tick — simplifica a gestão de estado e garante que os dados são sempre frescos sem necessidade de callbacks encadeados.
A adopção de **vistas Trino como interface de dados** entre o pipeline analítico e o dashboard tem três vantagens práticas. Primeiro, o dashboard recebe dados já agregados e filtrados — sem necessidade de transformações em Python além de conversões de tipo e formatação — o que simplifica o código de visualização e reduz a latência de renderização. Segundo, qualquer alteração ao modelo dimensional subjacente é absorvida na vista sem necessidade de alterar o dashboard. Terceiro, as mesmas vistas podem ser consumidas por outras ferramentas de BI (Metabase, Tableau, Grafana) sem alteração da camada de dados.
4.1.2 Correspondência entre Abas, Vistas e Perfis
A tabela seguinte apresenta a correspondência entre as seis abas do dashboard, as vistas Trino que as alimentam, o perfil de analista servido e a janela temporal de análise.

| **Aba** | **Vista(s) Trino** | **Perfil Principal** | **Janela Temporal** |
| --- | --- | --- | --- |
| Executivo | vw_executive | Gestor Executivo | 90 dias |
| Vendas | vw_sales_performance | Analista de Vendas | 90 dias |
| Funil | vw_funnel | Analista de Marketing | 7 dias |
| Reviews | vw_reviews | Gestor de Customer Experience | 90 dias |
| Tendências | vw_trends + vw_category_trends | Analista de Tendências | 60 dias / 30 dias |
| ML Insights | ml_demand_forecast | Data Scientist | Próximos 7 dias |

4.2 Serviços de Exploração e Análise Implementados
4.2.1 Aba Executivo
**Objetivo e Contexto**
A aba Executivo é o painel de entrada do sistema e serve o perfil do Gestor Executivo. Fornece uma visão de síntese do desempenho global da plataforma nos últimos 90 dias, combinando cinco KPIs de topo com quatro gráficos de série temporal que permitem identificar tendências e anomalias no desempenho comercial e na satisfação do cliente.
**Vista Subjacente**
A vista vw_executive agrega os dados diariamente ao nível global — sem desagregação por categoria ou região —, produzindo uma linha por dia com as colunas: total_orders, total_customers, total_revenue, avg_order_value, avg_rating, positive_reviews e negative_reviews. Esta granularidade diária é adequada para a análise executiva, que requer uma visão de tendência de longo prazo em vez de detalhe operacional.
**Métricas e Visualizações**
Os cinco **cartões KPI de topo** apresentam os agregados do período: Receita Total (sum de total_revenue), Total de Pedidos, Valor Médio por Pedido (média de avg_order_value), Rating Médio (média de avg_rating) e Clientes Únicos. Estes cartões são calculados em Python sobre o DataFrame resultante da query, sem necessidade de uma segunda query ao Trino.
O **gráfico de área de receita diária** apresenta a evolução temporal da receita total com preenchimento de área, permitindo identificar visualmente tendências de crescimento, picos e quedas no período.
O **gráfico de barras de pedidos por dia** apresenta o volume diário de encomendas, complementando a análise de receita — a diferença entre crescimento de receita e crescimento de pedidos pode indicar variação no valor médio por pedido.
O **gráfico de linha de rating médio diário** mostra a evolução da satisfação média dos clientes numa escala fixa [0, 5], permitindo identificar deteriorações de qualidade percebida antes que estas se traduzam em perda de vendas.
O **gráfico de barras empilhadas de ****reviews** apresenta o volume diário de avaliações positivas e negativas sobrepostas, permitindo monitorizar tanto o volume absoluto de feedback como a evolução da proporção de sentimento negativo.
**Valor Analítico**
Esta aba é o único ponto do dashboard que apresenta KPIs absolutos do período completo de 90 dias — todas as restantes abas desagregam os dados por alguma dimensão. O seu valor está na capacidade de dar uma resposta imediata à pergunta "como está o negócio hoje face à semana passada?", sem necessidade de navegar por filtros ou dimensões específicas.
4.2.2 Aba Vendas
**Objetivo e Contexto**
A aba Vendas serve o perfil do Analista de Vendas e fornece uma análise multidimensional do desempenho comercial desagregado por categoria de produto, estado e região geográfica. É o dashboard com maior profundidade de análise de negócio e o que mais directamente suporta decisões de portfólio de produto e estratégia regional.
**Vista Subjacente**
A vista vw_sales_performance opera ao nível da combinação (data, categoria, estado, região), expondo os campos: purchase_date, category, state, region, orders, customers, revenue, avg_order_value, product_revenue e freight_revenue. A granularidade ao nível da data permite ao dashboard agregar em qualquer dimensão superior (por categoria, por região, por período) sem perda de informação.
**Métricas e Visualizações**
O **gráfico de barras horizontais das top 10 categorias por receita** apresenta as dez categorias com maior contribuição para a receita no período, ordenadas de forma ascendente para facilitar a comparação visual. Este gráfico responde directamente à pergunta do Analista de Negócio sobre quais as categorias mais relevantes.
O **gráfico de donut por região** apresenta a distribuição percentual da receita pelas cinco macrorregiões brasileiras (Norte, Nordeste, Centro-Oeste, Sudeste, Sul), permitindo identificar a concentração geográfica da actividade comercial e regiões com potencial de crescimento subexplorado.
O **gráfico de linha de evolução da receita** apresenta a série temporal diária da receita total agregada sobre todas as dimensões, complementando o cartão de receita da aba Executivo com a granularidade temporal necessária para identificar picos sazonais e quebras de tendência.
O **gráfico de barras horizontais dos top 10 estados por receita** apresenta os dez estados brasileiros com maior contribuição para a receita em ordem decrescente, com granularidade geográfica mais fina do que o gráfico de donut regional.
O **gráfico de barras horizontais empilhadas de decomposição de receita** (RF2.4) decompõe a receita das top 10 categorias em product_revenue e freight_revenue, evidenciando a proporção do custo logístico por segmento de produto e identificando categorias onde o frete representa um peso desproporcional.
**Valor Analítico**
A combinação de análise por categoria e por geografia numa única aba permite ao Analista de Negócio correlacionar dois eixos de análise complementares: "quais as categorias mais vendidas?" e "onde se concentram essas vendas?". O **gráfico de barras horizontais empilhadas de decomposição de receita** (RF2.4) visualiza explicitamente a decomposição product_revenue / freight_revenue para as top 10 categorias, expondo a proporção de custo logístico por segmento de produto.
4.2.3 Aba Funil
**Objetivo e Contexto**
A aba Funil serve o perfil do Analista de Marketing e responde às perguntas sobre a eficácia do processo de conversão da plataforma — desde o início da sessão até à colocação da encomenda. Ao contrário das restantes abas, que operam com janelas de 90 dias, esta aba usa uma janela de 7 dias para garantir granularidade horária com volume de dados computacionalmente razoável.
**Vista Subjacente**
A vista vw_funnel agrega os eventos de clickstream com granularidade horária, produzindo uma linha por combinação de (hora, data, tipo de evento, dispositivo, categoria) com as colunas event_count, sessions e users. A granularidade horária é adequada para identificar padrões intra-diários no comportamento de navegação — por exemplo, picos de actividade ao fim do dia ou no intervalo de almoço.
**Métricas e Visualizações**
O **gráfico de funil de eventos** é a visualização central desta aba: apresenta os 11 tipos de evento nativos do clickstream mais o registo sintético order_placed adicionado via UNION ALL, na sua ordem natural de funil (session_start → search → category_browse → product_view → product_review_read → add_to_cart → remove_from_cart → cart_view → cart_abandon → checkout_start → order_placed → session_end), com a contagem absoluta e a percentagem face ao evento inicial. O formato de funil de Plotly torna imediatamente visível em que etapa ocorre a maior quebra de conversão.
O **gráfico de donut por dispositivo** apresenta a distribuição do total de eventos por tipo de dispositivo (mobile, desktop, tablet), permitindo ao Analista de Marketing identificar o canal dominante e avaliar se existem diferenças de comportamento significativas entre canais que justifiquem estratégias de optimização específicas.
O **gráfico de linha dos top 5 eventos ao longo do tempo** apresenta a evolução diária dos cinco tipos de evento com maior volume, permitindo identificar variações temporais no padrão de comportamento dos utilizadores — por exemplo, picos de add_to_cart sem crescimento correspondente de order_placed podem indicar problemas no processo de checkout.
O **gráfico de linha de sessões e utilizadores por dia** apresenta a evolução do número diário de sessões e de utilizadores únicos, permitindo distinguir crescimento de tráfego (mais sessões) de crescimento de base de utilizadores (mais utilizadores únicos) — dois fenómenos com implicações de negócio distintas.
**Valor Analítico**
Esta aba opera sobre dados gerados pelo simulador com probabilidades de funil fixas por construção, o que implica que as taxas de conversão são relativamente uniformes entre categorias e períodos. Em ambiente de produção real, com dados de clickstream genuínos, este dashboard seria o mais rico em insights operacionais — a heterogeneidade real das taxas de conversão entre categorias, dispositivos e períodos é tipicamente o principal driver de optimização de receita em plataformas de e-commerce.
4.2.4 Aba Reviews
**Objetivo e Contexto**
A aba Reviews serve o perfil do Gestor de Customer Experience e fornece uma análise da qualidade da experiência do cliente derivada das avaliações geradas após cada compra. É a única aba que explora a dimensão qualitativa do feedback do cliente, integrando a classificação de sentimento (positivo/neutro/negativo) derivada do rating na camada Gold.
**Vista Subjacente**
A vista vw_reviews agrega as avaliações por combinação de (data, sentimento, categoria, região), com as colunas review_count, avg_rating e avg_text_length. A coluna avg_text_length — comprimento médio do texto de avaliação — é um proxy do nível de detalhe e urgência percebida do feedback, tipicamente mais elevado em avaliações negativas onde o cliente se sente motivado a explicar a sua insatisfação.
**Métricas e Visualizações**
O **gráfico de donut de distribuição de sentimento** apresenta a proporção global de avaliações positivas, neutras e negativas no período de 90 dias, com código de cores consistente (verde/amarelo/vermelho) usado em todas as visualizações de sentimento da aba. Este gráfico responde directamente à pergunta do Gestor de Operações sobre a satisfação geral dos clientes.
O **gráfico de linha de rating médio ao longo do tempo** apresenta a evolução diária do rating médio ponderado pelo volume de reviews, com uma linha de referência horizontal em y=3 (limite neutro) e eixo Y fixo em [1, 5]. A ponderação pelo volume (em vez de média simples de médias) garante que dias com poucos reviews não distorcem a tendência visível.
O **gráfico de barras horizontais das top 10 categorias por número de ****reviews** apresenta as categorias com maior volume de feedback, com as barras coloridas por rating médio numa escala de gradiente vermelho-amarelo-verde (1 a 5). Esta visualização combina dois eixos de informação numa única representação: volume de feedback e qualidade percebida por categoria.
O **gráfico de barras empilhadas de sentimento por região** apresenta o volume de reviews por classe de sentimento para cada uma das cinco macrorregiões, permitindo ao Gestor de Operações identificar regiões com concentração anormalmente alta de reviews negativos que possam indicar problemas operacionais localizados.
**Valor Analítico**
Esta aba demonstra como dados qualitativos semi-estruturados (ficheiros de texto com rating e comentário) podem ser integrados num pipeline analítico completo — desde o ficheiro .txt no MinIO até à visualização em dashboard, passando por extracção de conteúdo em Silver e classificação de sentimento em Gold — e transformados em métricas quantificáveis com valor operacional directo.
4.2.5 Aba Tendências
**Objetivo e Contexto**
A aba Tendências serve o perfil do Analista de Tendências e fornece uma análise de dinâmica de mercado — não o que aconteceu em termos absolutos, mas a que velocidade e em que direcção as métricas estão a evoluir. É a única aba do dashboard que expõe métricas de segunda ordem (taxas de variação e aceleração) e sinalização de anomalias.
**Vistas Subjacentes**
A vista vw_trends agrega os dados diariamente ao nível global, calculando a taxa de crescimento week-over-week (WoW) de receita e pedidos, a aceleração da procura (variação da taxa de crescimento entre semanas consecutivas) e uma flag de anomalia para dias com desvio estatístico significativo face à média histórica. A janela de consulta é de 60 dias.
A vista vw_category_trends fornece as mesmas métricas de crescimento com desagregação por categoria, com uma janela mais curta de 30 dias para manter o volume de dados computacionalmente eficiente no dashboard.
**Métricas e Visualizações**
Os **quatro cartões KPI de tendência** apresentam os valores mais recentes das métricas-chave: crescimento de receita WoW (com cor verde/vermelho consoante sinal), crescimento de pedidos WoW, aceleração da procura, e o número total de anomalias detectadas nos últimos 60 dias.
O **gráfico de linha dupla de crescimento ****WoW** apresenta, para o período de 60 dias, as taxas de crescimento semanal de receita e pedidos em séries sobrepostas com linha de referência em y=0. Os dias classificados como anomalias são marcados com um ponto vermelho em forma de "X" sobre a linha de receita, tornando imediatamente visível quando o comportamento se desvia significativamente do padrão histórico.
O **gráfico de barras de aceleração da procura** apresenta, para cada dia, a diferença entre a taxa de crescimento actual e a da semana anterior — um valor positivo indica que o crescimento está a acelerar, um valor negativo que está a desacelerar. As barras são coloridas a verde (aceleração positiva) ou vermelho (aceleração negativa), e uma linha de referência em y=0 separa os dois regimes.
O **gráfico de linha de crescimento ****WoW**** por categoria** apresenta a evolução da taxa de crescimento de receita para as 8 categorias com maior receita total no período, permitindo comparar a trajectória de múltiplas categorias num único gráfico e identificar categorias em divergência face à tendência global.
O **gráfico de barras horizontais de crescimento médio por categoria** apresenta o crescimento médio WoW de cada categoria no período de 30 dias em barras horizontais, ordenadas por crescimento, com código de cores verde/vermelho. Este gráfico complementa o anterior ao mostrar um ranking estático de quais as categorias com melhor e pior desempenho relativo no período.
**Valor Analítico**
A aba Tendências é a única do dashboard que explora a **dinâmica** dos dados em vez do seu estado absoluto. A métrica de aceleração é particularmente relevante para o Analista de Marketing: uma categoria com crescimento positivo mas aceleração negativa está a entrar em maturação — o crescimento abranda — enquanto uma categoria com crescimento modesto mas aceleração positiva está em fase de emergência — o ritmo de crescimento está a aumentar. Esta distinção tem implicações directas para a alocação de orçamento de marketing e para as decisões de stock.
4.2.6 Aba ML Insights
**Objetivo e Contexto**
A aba ML Insights serve o perfil do Data Scientist (RF6.1–RF6.2) e apresenta os resultados do modelo de previsão de procura treinado diariamente pela DAG trendmart_ml_pipeline. É a única aba que consome directamente uma tabela Gold não coberta por vista Trino — lake.gold.ml_demand_forecast — e que expõe métricas de desempenho do modelo (RMSE, MAE) em conjunto com as previsões operacionais.
**Fonte de Dados**
A função get_demand_forecast() lê directamente a tabela lake.gold.ml_demand_forecast, devolvendo as previsões de pedidos para os próximos sete dias (D+1 a D+7) por categoria, com as métricas de avaliação do modelo (model_rmse, model_mae) incluídas como colunas em cada linha — o que permite apresentar as métricas de desempenho no mesmo painel que as previsões, sem necessidade de uma query separada ao MLflow.
**Métricas e Visualizações**
O **gráfico de linha de previsão de procura** apresenta as previsões de pedidos para D+1 a D+7, com uma série por categoria, marcadores nos pontos de previsão. As métricas RMSE e MAE do modelo são apresentadas como subtítulo abaixo do título do gráfico, fornecendo ao Data Scientist contexto imediato sobre a qualidade das previsões que está a consumir. Se a tabela ainda não estiver disponível — por exemplo antes da primeira execução da DAG ML —, a aba apresenta um alerta informativo em vez de um gráfico vazio.
**Valor Analítico**
Esta aba materializa o requisito RF6.1–RF6.2: transformar dados históricos de comportamento de compra em previsões accionáveis de curto prazo. O Data Scientist pode monitorizar a evolução das métricas de desempenho do modelo a cada execução diária da DAG sem necessidade de aceder ao MLflow, e utilizar as previsões de procura para suporte ao planeamento de stock e operações por categoria.

**5. O Sistema de Análise de Tendências de Aquisição de Produtos**
5.1 Definição do Problema e Compreensão dos Elementos de Análise Envolvidos
O sistema de análise de tendências de aquisição de produtos do TrendMart endereça o problema preditivo central do enunciado: a previsão de procura por categoria. O problema consiste em estimar, para cada categoria de produto, o número de ordens que serão geradas nos sete dias seguintes. A variável a prever é uma série temporal discreta não estacionária — o número diário de ordens por categoria — que apresenta padrões de sazonalidade semanal (comportamentos de compra distintos em dias úteis versus fim de semana), tendências de crescimento de longo prazo e variabilidade estocástica associada às condições específicas de cada categoria.
A relevância de negócio deste problema é imediata: previsões de procura com horizonte de sete dias permitem à gestão de operações antecipar necessidades de stock por categoria, planear campanhas de marketing em categorias com procura crescente prevista, e identificar antecipadamente categorias com procura declinante que podem justificar acções correctivas de preço ou promoção.
5.2 Seleção e Preparação dos Dados
5.2.1 Dados para Previsão de Procura
Os dados de input para o modelo de previsão de procura são construídos pelo módulo machine_learning/features/timeseries_features.py a partir das tabelas Gold fact_sales, dim_date e dim_category.
O processo de preparação inicia com uma agregação diária por categoria: para cada par (categoria, data de compra), são calculados o número de ordens distintas (orders_count) e a receita total (total_revenue). Esta agregação, executada via Spark SQL com GROUP BY category_id, purchase_date, produz uma série temporal diária por categoria.
Sobre esta série temporal são calculadas as seguintes **features**** de ****lag**** e de janela deslizante**:

| **Feature** | **Cálculo** | **Interpretação** |
| --- | --- | --- |
| lag_1 | orders_count do dia anterior | Autocorrelação de ordem 1 |
| lag_7 | orders_count de há 7 dias | Padrão semanal — mesmo dia da semana |
| lag_14 | orders_count de há 14 dias | Padrão semanal com 2 semanas de histórico |
| rolling_7d_mean | Média dos últimos 7 dias | Tendência de curto prazo |

São ainda incluídas **features**** de calendário** derivadas da dimensão dim_date: day_of_week, is_weekend, month e quarter. Estas features permitem ao modelo capturar a sazonalidade semanal e mensal sem necessidade de dados externos.
As linhas para as quais o lag_14 não está disponível (os primeiros 14 dias de histórico de cada categoria) são descartadas, garantindo que todos os registos de treino têm o conjunto completo de features. Categorias com menos de 20 registos após este filtro são excluídas do treino por insuficiência de dados.

5.3 Identificação e Fundamentação da Técnica de Análise
5.3.1 Previsão de Procura — Regressão Linear com Lag Features
A técnica seleccionada para a previsão de procura é uma **Regressão Linear** (Spark MLlib LinearRegression) treinada separadamente para cada categoria de produto, com as oito features de lag e calendário descritas na secção anterior.
A escolha de Regressão Linear em detrimento de alternativas mais complexas como modelos de séries temporais clássicos (ARIMA, Prophet) ou modelos de gradient boosting (XGBoost) justifica-se por quatro razões complementares.
Primeiro, a **interpretabilidade**: os coeficientes do modelo linear são directamente interpretáveis — o coeficiente de lag_7 representa o impacto que mais uma ordem há sete dias tem na previsão de hoje, o que tem significado analítico directo para o negócio. Um modelo de gradient boosting produziria previsões potencialmente mais precisas mas com muito menor interpretabilidade.
Segundo, a **robustez com dados limitados**: muitas categorias do simulador têm um histórico de dados relativamente curto. A Regressão Linear, com apenas oito parâmetros, é menos propensa a overfitting do que modelos de maior capacidade num regime de poucos dados.
Terceiro, a **integração nativa com ****Spark**** ****MLlib**: a utilização de LinearRegression do MLlib permite treinar e fazer inferência directamente sobre DataFrames Spark, sem necessidade de converter dados para formatos externos (pandas, numpy) ou lançar processos adicionais. Este aspecto é particularmente relevante num contexto de produção onde o pipeline de dados já corre em Spark.
Quarto, a **escalabilidade**: o modelo é treinado independentemente por categoria, o que permite uma paralelização trivial — cada categoria pode ser treinada num executor Spark diferente — e facilita a adição de novas categorias sem reprocessar as existentes.
O hiperparâmetro de regularização regParam=0.1 (regularização L2/Ridge) foi fixado para prevenir coeficientes excessivamente grandes nas categorias com menor volume de dados, sem necessidade de validação cruzada completa — uma decisão que simplifica a implementação em troca de uma potencial suboptimalidade na regularização para categorias específicas.

5.4 Construção do Modelo de Análise
5.4.1 Pipeline de Previsão de Procura
O modelo de previsão de procura é implementado como um Pipeline Spark MLlib com dois stages: um VectorAssembler que agrega as oito features em único vector de entrada, e um LinearRegression com maxIter=100 e regParam=0.1.
O treino é executado por categoria, iterando sobre a lista de categorias distintas presentes na tabela de features. Para cada categoria com dados suficientes (≥ 20 registos pós-filtro), é aplicada uma **divisão cronológica** de 80/20 para treino e teste: os primeiros 80% das datas por ordem cronológica formam o conjunto de treino, e os últimos 20% formam o conjunto de teste. Esta divisão cronológica é essencial em séries temporais — uma divisão aleatória causaria data leakage, pois o modelo poderia "ver" dados futuros durante o treino.
Após o treino e avaliação, o modelo é aplicado a um conjunto de **features**** futuras** construídas pelo método _make_future_rows, que gera sete linhas (uma por dia D+1 a D+7) com as features populadas a partir dos últimos 14 dias de histórico real. Os valores de lag são extraídos directamente do histórico recente — lag_1 é o número de ordens de ontem, lag_7 é o de há sete dias — o que garante que as previsões futuras são baseadas em dados reais e não em extrapolações dos próprios outputs do modelo. As previsões negativas são clipadas a zero por aplicação de GREATEST(prediction, 0).
O modelo é registado no **MLflow** com os parâmetros de treino, as métricas de avaliação e o artefacto do modelo Spark para auditoria e comparação histórica de runs.
5.5 Validação do Desempenho do Modelo
5.5.1 Validação do Modelo de Previsão de Procura
O desempenho do modelo de previsão de procura é avaliado no conjunto de teste (últimos 20% das datas por categoria) com duas métricas complementares:
**RMSE (****Root**** ****Mean**** ****Squared**** Error)**: mede o erro quadrático médio das previsões, penalizando erros grandes de forma quadrática. Um RMSE de _x_ ordens significa que o modelo tem em média um erro do quadrado de _x_ ordens nas suas previsões sobre o conjunto de teste.
**MAE (****Mean**** ****Absolute**** Error)**: mede o erro absoluto médio das previsões, sem penalização quadrática. O MAE é mais robusto a outliers do que o RMSE e é mais interpretável directamente: um MAE de _x_ ordens significa que o modelo erra em média _x_ ordens por dia.
As métricas são calculadas por categoria e a seguir agregadas em médias globais para uma visão do desempenho geral do sistema. Ambas as métricas são registadas no MLflow por run de categoria, permitindo comparação histórica do desempenho à medida que mais dados ficam disponíveis.
A divisão cronológica garante que o conjunto de teste contém apenas dados posteriores ao treino, tornando as métricas representativas do desempenho esperado em produção para o horizonte de uma a duas semanas.

5.6 Avaliação dos Resultados
Os resultados do sistema de análise de tendências de aquisição de produtos são disponibilizados em dois formatos complementares.
**Dashboard ML Insights**: a aba ML Insights do dashboard Dash apresenta, em modo de auto-refresh de 5 minutos, as previsões de procura para os próximos sete dias por categoria (tabela ml_demand_forecast) e as métricas de desempenho mais recentes do modelo (RMSE e MAE). Esta visualização permite ao Data Scientist monitorizar o desempenho do modelo a cada execução diária da DAG ML sem necessidade de acesso directo ao Trino ou ao MLflow.
**MLflow Experiment Tracking**: o MLflow regista, para cada execução diária da DAG, um run por categoria no experiment demand_forecasting, com os parâmetros de treino (regParam, maxIter), as métricas de avaliação (RMSE, MAE) e o artefacto do modelo Spark. Este registo permite auditar a evolução do desempenho do modelo ao longo do tempo e detectar degradação de performance (model drift) à medida que o padrão de dados do simulador evolui.
A limitação principal dos resultados actuais decorre da natureza sintética dos dados do simulador: as séries temporais de ordens por categoria são geradas com distribuições probabilísticas relativamente uniformes e sem sazonalidade real, o que tende a produzir modelos com RMSE baixo mas sem capacidade de capturar padrões de sazonalidade anual expectáveis em dados reais de e-commerce. Em ambiente de produção com dados históricos reais, seria recomendável avaliar modelos com maior capacidade de capturar sazonalidade, como o Prophet ou o XGBoost com features de Fourier para sazonalidade.

**6. Conclusões e Trabalho Futuro**
6.1 Conclusões
*[A preencher]*
6.2 Trabalho Futuro
*[A preencher]*

*[Anexo A — Diagrama BPMN do Processo de Integração de Dados — a inserir]*
