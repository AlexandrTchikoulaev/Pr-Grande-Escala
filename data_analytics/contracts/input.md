# data_analytics — Contrato de Entrada

Inputs consumidos de `analytical_engineering`. Ver [analytical_engineering/contracts/output.md](../../analytical_engineering/contracts/output.md).

## Vistas Trino consumidas

| Vista | Descrição |
|-------|-----------|
| `lake.gold.vw_executive` | KPIs diários globais de receita, encomendas e satisfação para o Gestor Executivo. |
| `lake.gold.vw_sales_performance` | Desempenho de vendas por categoria e região para o Analista de Vendas. |
| `lake.gold.vw_funnel` | Funil de eventos de navegação e distribuição por dispositivo para o Analista de Marketing. |
| `lake.gold.vw_reviews` | Sentimento e classificação de avaliações por categoria e região para o Gestor de CX. |
| `lake.gold.vw_trends` | Crescimento semanal, aceleração da procura e anomalias para o Analista de Tendências. |
| `lake.gold.ml_demand_forecast` | Previsões de procura por categoria (D+1 a D+7) e métricas de desempenho do modelo para o Data Scientist. |

## Perfis de utilizador servidos

| Perfil | Necessidade central |
|--------|---------------------|
| Gestor Executivo | Monitorizar a saúde global do negócio através de KPIs de alto nível. |
| Analista de Vendas | Avaliar o desempenho comercial por categoria, região e tempo. |
| Analista de Marketing | Analisar o comportamento de navegação e eficácia do funil de conversão. |
| Gestor de Customer Experience | Identificar padrões de satisfação e insatisfação através de avaliações. |
| Analista de Tendências | Detetar padrões de crescimento e anomalias no comportamento de vendas. |
| Data Scientist | Interpretar previsões de procura e monitorizar a qualidade do modelo ML. |
