# data_analytics — Contrato de Saída

Dashboards interativos expostos aos seis perfis de analistas de dados.

## Dashboards

| Tab | Perfil | Saída |
|-----|--------|-------|
| Executivo | Gestor Executivo | KPIs globais e evolução diária de receita, encomendas e satisfação nos últimos 90 dias. |
| Vendas | Analista de Vendas | Top 10 categorias, distribuição geográfica de receita e decomposição produto/frete. |
| Funil | Analista de Marketing | Funil de eventos de clickstream, distribuição por dispositivo e evolução de sessões nos últimos 7 dias. |
| Reviews | Gestor de CX | Distribuição de sentimento, evolução da classificação média e análise por categoria e região. |
| Tendências | Analista de Tendências | Crescimento semana-a-semana, aceleração da procura e anomalias detetadas nos últimos 60 dias. |
| ML Insights | Data Scientist | Previsão de encomendas para os próximos 7 dias por categoria e métricas RMSE/MAE do modelo. |

## SLA de atualização

Os dashboards refletem os dados com latência máxima de **1 hora** (cadência do pipeline Gold) e são refrescados automaticamente a cada **5 minutos**.
