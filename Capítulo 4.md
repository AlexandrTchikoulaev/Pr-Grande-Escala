# 4. Exploração e Análise de Dados

## 4.1 Organização Geral do Sistema de Dashboarding

A camada de visualização do TrendMart é implementada como uma aplicação web em Plotly Dash com Bootstrap, organizada em seis dashboards temáticos, cada um concebido para responder às perguntas de negócio de um perfil de analista caracterizado no capítulo 2. A aplicação está acessível em http://localhost:8050 e actualiza automaticamente os seus dados a cada cinco minutos via um componente `dcc.Interval`, garantindo que a informação apresentada reflecte o estado do pipeline Gold sem necessidade de intervenção manual.

### 4.1.1 Arquitectura da Camada de Visualização

A aplicação é composta por dois módulos Python distintos com responsabilidades claramente separadas.

O módulo `data.py` constitui a camada de dados: contém exclusivamente funções que constroem queries SQL parametrizadas, as executam sobre as vistas Trino via o cliente `trino.dbapi`, e devolvem DataFrames Pandas. Cada função corresponde a um dashboard — `get_executive`, `get_sales_performance`, `get_funnel`, `get_reviews`, `get_trends`, `get_category_trends`, `get_demand_forecast` — e recebe como parâmetro a janela temporal desejada em dias. Esta separação garante que a lógica de acesso a dados pode ser testada, substituída ou extendida sem alterar a camada de apresentação.

O módulo `app.py` constitui a camada de apresentação: define o layout da aplicação com navegação por tabs (`dcc.Tabs`) entre os seis dashboards, regista o callback principal que responde a mudanças de dashboard e ao tick de refresh, e delega a construção de cada dashboard para funções de renderização especializadas (`_tab_executive`, `_tab_sales`, `_tab_funnel`, `_tab_reviews`, `_tab_trends`, `_tab_ml`). A arquitectura de callback único — um único `@app.callback` que re-renderiza o conteúdo completo do dashboard activo a cada tick — simplifica a gestão de estado e garante que os dados são sempre frescos sem necessidade de callbacks encadeados.

A adopção de vistas Trino como interface de dados entre o pipeline analítico e os dashboards tem três vantagens práticas. Primeiro, cada dashboard recebe dados já agregados e filtrados — sem necessidade de transformações em Python além de conversões de tipo e formatação —, o que simplifica o código de visualização e reduz a latência de renderização. Segundo, qualquer alteração ao modelo dimensional subjacente é absorvida na vista sem necessidade de alterar os dashboards. Terceiro, as mesmas vistas podem ser consumidas por outras ferramentas de BI (Metabase, Tableau, Grafana) sem alteração da camada de dados.

### 4.1.2 Correspondência entre Dashboards, Vistas e Perfis

A tabela seguinte apresenta a correspondência entre os seis dashboards, as vistas Trino que os alimentam, o perfil de analista servido e a janela temporal de análise.

| Dashboard | Vista(s) Trino | Perfil Principal | Janela Temporal |
|-----------|----------------|------------------|-----------------|
| Executivo | vw_executive | Gestor Executivo | 90 dias |
| Vendas | vw_sales_performance | Analista de Vendas | 90 dias |
| Funil | vw_funnel | Analista de Marketing | 7 dias |
| Reviews | vw_reviews | Gestor de Customer Experience | 90 dias |
| Tendências | vw_trends + vw_category_trends | Analista de Tendências | 60 dias / 30 dias |
| ML Insights | ml_demand_forecast | Data Scientist | Próximos 7 dias |


## 4.2 Dashboards Implementados

### 4.2.1 Dashboard Executivo

#### Objetivo e Contexto

O Dashboard Executivo é o painel de entrada do sistema e serve o perfil do Gestor Executivo (RF1.1–RF1.4). Fornece uma visão de síntese do desempenho global da plataforma nos últimos 90 dias, combinando cinco KPIs de topo com quatro gráficos de série temporal que permitem identificar tendências e anomalias no desempenho comercial e na satisfação do cliente.

#### Métricas e Visualizações

Os cinco cartões KPI de topo apresentam os agregados do período: Receita Total, Total de Pedidos, Valor Médio por Pedido, Rating Médio e Clientes Únicos. Estes cartões são calculados em Python sobre o DataFrame resultante da query, sem necessidade de uma segunda query ao Trino.

O gráfico de área de receita diária apresenta a evolução temporal da receita total com preenchimento de área, permitindo identificar visualmente tendências de crescimento, picos e quedas no período.

O gráfico de barras de pedidos por dia apresenta o volume diário de encomendas, complementando a análise de receita — a diferença entre crescimento de receita e crescimento de pedidos pode indicar variação no valor médio por pedido.

O gráfico de linha de rating médio diário mostra a evolução da satisfação média dos clientes numa escala fixa [0, 5], permitindo identificar deteriorações de qualidade percebida antes que estas se traduzam em perda de vendas.

O gráfico de barras empilhadas de reviews apresenta o volume diário de avaliações positivas e negativas sobrepostas, permitindo monitorizar tanto o volume absoluto de feedback como a evolução da proporção de sentimento negativo.

#### Valor Analítico

Este dashboard é o único ponto da aplicação que apresenta KPIs absolutos do período completo de 90 dias — todos os restantes dashboards desagregam os dados por alguma dimensão. O seu valor está na capacidade de dar uma resposta imediata à pergunta "como está o negócio hoje face à semana passada?", sem necessidade de navegar por filtros ou dimensões específicas. Responde directamente aos requisitos RF1.1 (KPIs globais diários), RF1.2 (evolução diária da receita), RF1.3 (volume diário de reviews positivas e negativas) e RF1.4 (rating médio diário).

### 4.2.2 Dashboard Vendas

#### Objetivo e Contexto

O Dashboard Vendas serve o perfil do Analista de Vendas (RF2.1–RF2.4) e fornece uma análise multidimensional do desempenho comercial desagregado por categoria de produto, estado e região geográfica. É o dashboard com maior profundidade de análise de negócio e o que mais directamente suporta decisões de portfólio de produto e estratégia regional.

#### Métricas e Visualizações

O gráfico de barras horizontais das top 10 categorias por receita apresenta as dez categorias com maior contribuição para a receita no período, ordenadas de forma ascendente para facilitar a comparação visual.

O gráfico de donut por região apresenta a distribuição percentual da receita pelas cinco macrorregiões brasileiras (Norte, Nordeste, Centro-Oeste, Sudeste, Sul), permitindo identificar a concentração geográfica da actividade comercial e regiões com potencial de crescimento subexplorado.

O gráfico de linha de evolução da receita apresenta a série temporal diária da receita total agregada sobre todas as dimensões, complementando o cartão de receita do Dashboard Executivo com a granularidade temporal necessária para identificar picos sazonais e quebras de tendência.

O gráfico de barras dos top 10 estados por receita apresenta os dez estados brasileiros com maior contribuição para a receita em ordem decrescente, com granularidade geográfica mais fina do que o gráfico de donut regional.

#### Valor Analítico

A combinação de análise por categoria e por geografia num único dashboard permite ao Analista de Vendas correlacionar dois eixos de análise complementares: "quais as categorias mais vendidas?" e "onde se concentram essas vendas?". A decomposição `product_revenue` / `freight_revenue` disponível na vista está acessível para análises ad hoc via Trino. Responde aos requisitos RF2.1 (receita por categoria e região), RF2.2 (contagem de clientes únicos por dia), RF2.3 (impacto do frete no valor da ordem) e RF2.4 (volume de ordens por período).

### 4.2.3 Dashboard Funil

#### Objetivo e Contexto

O Dashboard Funil serve o perfil do Analista de Marketing (RF3.1–RF3.4) e responde às perguntas sobre a eficácia do processo de conversão da plataforma — desde o início da sessão até à colocação da encomenda. Ao contrário dos restantes dashboards, que operam com janelas de 90 dias, este utiliza uma janela de 7 dias para garantir granularidade horária com volume de dados computacionalmente razoável.

#### Métricas e Visualizações

O gráfico de funil de eventos é a visualização central deste dashboard: apresenta os tipos de evento do funil na sua ordem natural — `session_start`, `search`, `category_browse`, `product_view`, `product_review_read`, `add_to_cart`, `remove_from_cart`, `cart_view`, `cart_abandon`, `checkout_start`, `order_placed`, `session_end` — com a contagem absoluta e a percentagem face ao evento inicial. O formato de funil torna imediatamente visível em que etapa ocorre a maior quebra de conversão.

O gráfico de donut por dispositivo apresenta a distribuição do total de eventos por tipo de dispositivo (mobile, desktop, tablet), permitindo ao Analista de Marketing identificar o canal dominante e avaliar se existem diferenças de comportamento significativas entre canais.

O gráfico de linha dos top 5 eventos ao longo do tempo apresenta a evolução diária dos cinco tipos de evento com maior volume, permitindo identificar variações temporais no padrão de comportamento — por exemplo, picos de `add_to_cart` sem crescimento correspondente de `order_placed` podem indicar problemas no processo de checkout.

O gráfico de linha de sessões e utilizadores por dia apresenta a evolução do número diário de sessões e de utilizadores únicos, permitindo distinguir crescimento de tráfego (mais sessões) de crescimento de base de utilizadores (mais utilizadores únicos).

#### Valor Analítico

Este dashboard opera sobre dados gerados pelo simulador com probabilidades de funil fixas por construção, o que implica que as taxas de conversão são relativamente uniformes entre categorias e períodos. Em ambiente de produção real, com dados de clickstream genuínos, seria o dashboard mais rico em insights operacionais — a heterogeneidade real das taxas de conversão entre categorias, dispositivos e períodos é tipicamente o principal driver de optimização de receita em plataformas de e-commerce. Responde aos requisitos RF3.1 (análise do funil), RF3.2 (preferências de dispositivo), RF3.3 (padrões de navegação por hora) e RF3.4 (categorias mais exploradas).

### 4.2.4 Dashboard Reviews

#### Objetivo e Contexto

O Dashboard Reviews serve o perfil do Gestor de Customer Experience (RF4.1–RF4.4) e fornece uma análise da qualidade da experiência do cliente derivada das avaliações geradas após cada compra. É o único dashboard que explora a dimensão qualitativa do feedback do cliente, integrando a classificação de sentimento (positivo/neutro/negativo) derivada do rating na camada Gold.

#### Métricas e Visualizações

O gráfico de donut de distribuição de sentimento apresenta a proporção global de avaliações positivas, neutras e negativas no período de 90 dias, com código de cores consistente (verde/amarelo/vermelho) usado em todas as visualizações de sentimento do dashboard.

O gráfico de linha de rating médio ao longo do tempo apresenta a evolução diária do rating médio ponderado pelo volume de reviews, com uma linha de referência horizontal em y=3 (limite neutro) e eixo Y fixo em [1, 5].

O gráfico de barras horizontais das top 10 categorias por número de reviews apresenta as categorias com maior volume de feedback, com as barras coloridas por rating médio numa escala de gradiente vermelho-amarelo-verde (1 a 5). Esta visualização combina dois eixos de informação numa única representação: volume de feedback e qualidade percebida por categoria.

O gráfico de barras empilhadas de sentimento por região apresenta o volume de reviews por classe de sentimento para cada uma das cinco macrorregiões, permitindo identificar regiões com concentração anormalmente alta de reviews negativos que possam indicar problemas operacionais localizados.

#### Valor Analítico

Este dashboard demonstra como dados qualitativos não estruturados (ficheiros de texto com rating e comentário) podem ser integrados num pipeline analítico completo — desde o ficheiro `.txt` no MinIO até à visualização em dashboard, passando por extracção de conteúdo em Silver e classificação de sentimento em Gold — e transformados em métricas quantificáveis com valor operacional directo. Responde aos requisitos RF4.1 (volume e rating médio por categoria), RF4.2 (evolução do sentimento ao longo do tempo), RF4.3 (categorias com pior satisfação) e RF4.4 (distribuição geográfica das avaliações).

### 4.2.5 Dashboard Tendências

#### Objetivo e Contexto

O Dashboard Tendências serve o perfil do Analista de Tendências (RF5.1–RF5.4) e fornece uma análise de dinâmica de mercado — não o que aconteceu em termos absolutos, mas a que velocidade e em que direcção as métricas estão a evoluir. É o único dashboard que expõe métricas de segunda ordem (taxas de variação e aceleração) e sinalização de anomalias estatísticas.

#### Métricas e Visualizações

Os quatro cartões KPI de tendência apresentam os valores mais recentes das métricas-chave: crescimento de receita WoW (com cor verde/vermelho consoante sinal), crescimento de pedidos WoW, aceleração da procura e o número total de anomalias detectadas nos últimos 60 dias.

O gráfico de linha dupla de crescimento WoW apresenta, para o período de 60 dias, as taxas de crescimento semanal de receita e pedidos em séries sobrepostas com linha de referência em y=0. Os dias classificados como anomalias são marcados com um ponto vermelho em forma de "X" sobre a linha de receita.

O gráfico de barras de aceleração da procura apresenta, para cada dia, a diferença entre a taxa de crescimento actual e a da semana anterior — um valor positivo indica que o crescimento está a acelerar, um valor negativo que está a desacelerar. As barras são coloridas a verde (aceleração positiva) ou vermelho (aceleração negativa).

O gráfico de linha de crescimento WoW por categoria apresenta a evolução da taxa de crescimento de receita para as 8 categorias com maior receita total no período, permitindo comparar a trajectória de múltiplas categorias num único gráfico.

O gráfico de barras horizontais de crescimento médio por categoria apresenta o crescimento médio WoW de cada categoria no período de 30 dias, ordenadas por crescimento, com código de cores verde/vermelho.

#### Valor Analítico

O Dashboard Tendências é o único que explora a dinâmica dos dados em vez do seu estado absoluto. A métrica de aceleração é particularmente relevante para o Analista de Marketing: uma categoria com crescimento positivo mas aceleração negativa está a entrar em maturação, enquanto uma categoria com crescimento modesto mas aceleração positiva está em fase de emergência. Esta distinção tem implicações directas para a alocação de orçamento de marketing e para as decisões de stock. Responde aos requisitos RF5.1 (tendências de crescimento), RF5.2 (aceleração da procura), RF5.3 (detecção de anomalias) e RF5.4 (crescimento por categoria).

### 4.2.6 Dashboard ML Insights

#### Objetivo e Contexto

O Dashboard ML Insights serve o perfil do Data Scientist (RF6.1–RF6.2) e apresenta os resultados do modelo de previsão de procura treinado diariamente pela DAG `trendmart_ml_pipeline`. É o único dashboard que consome directamente uma tabela Gold não coberta por vista Trino — `lake.gold.ml_demand_forecast` — e que expõe métricas de desempenho do modelo (RMSE, MAE) em conjunto com as previsões operacionais.

#### Métricas e Visualizações

O gráfico de linha de previsão de procura apresenta as previsões de pedidos para D+1 a D+7, com uma série por categoria e marcadores nos pontos de previsão. As métricas RMSE e MAE do modelo são apresentadas como subtítulo abaixo do título do gráfico, fornecendo ao Data Scientist contexto imediato sobre a qualidade das previsões que está a consumir. Se a tabela ainda não estiver disponível — por exemplo antes da primeira execução da DAG ML —, o dashboard apresenta um alerta informativo em vez de um gráfico vazio.

#### Valor Analítico

Este dashboard materializa os requisitos RF6.1–RF6.2: transformar dados históricos de comportamento de compra em previsões accionáveis de curto prazo. O Data Scientist pode monitorizar a evolução das métricas de desempenho do modelo a cada execução diária da DAG sem necessidade de aceder ao MLflow, e utilizar as previsões de procura para suporte ao planeamento de stock e operações por categoria.
