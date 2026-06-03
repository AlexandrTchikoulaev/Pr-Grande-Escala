import re

path = r"c:\Users\Alexandr\Desktop\Universidade\3º Ano\Grande Escala\Projeto Versão Final\ProjetoGrandeEscala\relatorio.md"

with open(path, encoding="utf-8") as f:
    content = f.read()

# ── 5.1: replace intro + remove churn subsection ──────────────────────────
marker_51_start = "O sistema de análise de tendências de aquisição de produtos do TrendMart endereça dois problemas preditivos complementares"
marker_51_end = "em vez de crescimento orgânico da base de clientes fiel."

new_51 = (
    "O sistema de análise de tendências de aquisição de produtos do TrendMart endereça o problema preditivo "
    "central do enunciado: a previsão de procura por categoria. O problema consiste em estimar, para cada "
    "categoria de produto, o número de ordens que serão geradas nos sete dias seguintes. A variável a prever "
    "é uma série temporal discreta não estacionária — o número diário de ordens por categoria — que apresenta "
    "padrões de sazonalidade semanal (comportamentos de compra distintos em dias úteis versus fim de semana), "
    "tendências de crescimento de longo prazo e variabilidade estocástica associada às condições específicas "
    "de cada categoria.\n"
    "A relevância de negócio deste problema é imediata: previsões de procura com horizonte de sete dias "
    "permitem à gestão de operações antecipar necessidades de stock por categoria, planear campanhas de "
    "marketing em categorias com procura crescente prevista, e identificar antecipadamente categorias com "
    "procura declinante que podem justificar acções correctivas de preço ou promoção."
)

idx_start = content.find(marker_51_start)
idx_end   = content.find(marker_51_end)
if idx_start != -1 and idx_end != -1:
    content = content[:idx_start] + new_51 + content[idx_end + len(marker_51_end):]
    print("5.1 fixed")
else:
    print(f"5.1 NOT found: start={idx_start}, end={idx_end}")

# ── 5.2.2: remove entire subsection ───────────────────────────────────────
marker_522_start = "5.2.2 Dados para Detecção de Churn"
marker_522_end   = "de ajuste em produção com dados reais."

idx_start = content.find(marker_522_start)
idx_end   = content.find(marker_522_end)
if idx_start != -1 and idx_end != -1:
    content = content[:idx_start] + content[idx_end + len(marker_522_end):]
    print("5.2.2 removed")
else:
    print(f"5.2.2 NOT found: start={idx_start}, end={idx_end}")

# ── 5.3.2: remove entire subsection ───────────────────────────────────────
marker_532_start = "5.3.2 Detecção de Churn"
marker_532_end   = "comunicar esses resultados de forma interpretável ao negócio."

idx_start = content.find(marker_532_start)
idx_end   = content.find(marker_532_end)
if idx_start != -1 and idx_end != -1:
    content = content[:idx_start] + content[idx_end + len(marker_532_end):]
    print("5.3.2 removed")
else:
    print(f"5.3.2 NOT found: start={idx_start}, end={idx_end}")

# ── 5.4.2: remove and replace with MLflow note ────────────────────────────
marker_542_start = "5.4.2 Pipeline de Detecção de Churn"
marker_542_end   = "comparação histórica de runs."

idx_start = content.find(marker_542_start)
idx_end   = content.find(marker_542_end)
if idx_start != -1 and idx_end != -1:
    replacement = "O modelo é registado no **MLflow** com os parâmetros de treino, as métricas de avaliação e o artefacto do modelo Spark para auditoria e comparação histórica de runs."
    content = content[:idx_start] + replacement + content[idx_end + len(marker_542_end):]
    print("5.4.2 replaced")
else:
    print(f"5.4.2 NOT found: start={idx_start}, end={idx_end}")

# ── 5.5.2: remove entire subsection ───────────────────────────────────────
marker_552_start = "5.5.2 Validação do Modelo de Churn"
marker_552_end   = "contribuição para a previsão de churn."

idx_start = content.find(marker_552_start)
idx_end   = content.find(marker_552_end)
if idx_start != -1 and idx_end != -1:
    content = content[:idx_start] + content[idx_end + len(marker_552_end):]
    print("5.5.2 removed")
else:
    print(f"5.5.2 NOT found: start={idx_start}, end={idx_end}")

# ── 5.6: rewrite without churn ────────────────────────────────────────────
marker_56_start = "Os resultados do sistema de análise de tendências de aquisição de produtos são disponibilizados em dois formatos complementares."
marker_56_end   = "como o Prophet ou o XGBoost com features de Fourier para sazonalidade."

new_56 = (
    "Os resultados do sistema de análise de tendências de aquisição de produtos são disponibilizados em dois "
    "formatos complementares.\n"
    "**Dashboard ML Insights**: a aba ML Insights do dashboard Dash apresenta, em modo de auto-refresh de "
    "5 minutos, as previsões de procura para os próximos sete dias por categoria (tabela ml_demand_forecast) "
    "e as métricas de desempenho mais recentes do modelo (RMSE e MAE). Esta visualização permite ao Data "
    "Scientist monitorizar o desempenho do modelo a cada execução diária da DAG ML sem necessidade de acesso "
    "directo ao Trino ou ao MLflow.\n"
    "**MLflow Experiment Tracking**: o MLflow regista, para cada execução diária da DAG, um run por categoria "
    "no experiment demand_forecasting, com os parâmetros de treino (regParam, maxIter), as métricas de "
    "avaliação (RMSE, MAE) e o artefacto do modelo Spark. Este registo permite auditar a evolução do "
    "desempenho do modelo ao longo do tempo e detectar degradação de performance (model drift) à medida que "
    "o padrão de dados do simulador evolui.\n"
    "A limitação principal dos resultados actuais decorre da natureza sintética dos dados do simulador: as "
    "séries temporais de ordens por categoria são geradas com distribuições probabilísticas relativamente "
    "uniformes e sem sazonalidade real, o que tende a produzir modelos com RMSE baixo mas sem capacidade de "
    "capturar padrões de sazonalidade anual expectáveis em dados reais de e-commerce. Em ambiente de produção "
    "com dados históricos reais, seria recomendável avaliar modelos com maior capacidade de capturar "
    "sazonalidade, como o Prophet ou o XGBoost com features de Fourier para sazonalidade."
)

idx_start = content.find(marker_56_start)
idx_end   = content.find(marker_56_end)
if idx_start != -1 and idx_end != -1:
    content = content[:idx_start] + new_56 + content[idx_end + len(marker_56_end):]
    print("5.6 fixed")
else:
    print(f"5.6 NOT found: start={idx_start}, end={idx_end}")

with open(path, "w", encoding="utf-8") as f:
    f.write(content)

print("Done")
