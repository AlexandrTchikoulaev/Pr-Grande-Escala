# Data Sources — Estudo para Defesa

## O que é e para que serve

O Data Sources é o ponto de entrada de todos os dados do sistema. Como não existe uma plataforma de e-commerce real, esta equipa implementou um **simulador** que gera dados contínuos, replicando o comportamento real de utilizadores numa loja online. O simulador usa dados reais do **dataset Olist** — um dataset público de e-commerce brasileiro — como referência para produtos, clientes, preços e textos de reviews. Os dados Olist são lidos uma única vez na inicialização e ficam em memória; o que o simulador gera são eventos novos com IDs únicos, não cópias dos dados históricos.

---

## Estrutura de ficheiros e responsabilidade de cada um

### `main.py` — Entry point

É o loop principal. Inicializa as três conexões (Kafka producer, PostgreSQL, MinIO), carrega os dados de referência, e corre um loop infinito que:

1. Chama `simulate_session()` para gerar uma sessão
2. Aplica ruído via `noise.py` em cada output
3. Distribui os resultados pelos três destinos
4. Dorme 2 segundos (0.5 sessões/segundo)

Tem shutdown gracioso com `SIGINT`/`SIGTERM`: faz flush do Kafka e fecha a ligação à base de dados antes de terminar.

---

### `loader.py` — Carregamento dos CSVs

Lê cinco ficheiros do dataset Olist na inicialização:

- `olist_products_dataset.csv` — pool de produtos
- `olist_customers_dataset.csv` — pool de clientes
- `olist_order_items_dataset.csv` — preços, sellers e fretes por produto
- `olist_order_reviews_dataset.csv` — textos reais de reviews (só as que têm mensagem)
- `product_category_name_translation.csv` — tradução de categorias PT→EN

Constrói índices em memória e expõe `pick_product()`, `pick_customer()` e `pick_review_text()`, que fazem `random.choice` sobre os pools. Usa encoding `utf-8-sig` para lidar com BOM nos CSVs.

---

### `session.py` — Simulação do funil

Simula uma sessão completa de um utilizador. Gera UUIDs para `session_id`, `event_id`, `order_id` e `review_id`. A sessão segue um funil com probabilidades configuráveis:

```
session_start
  → search (60%) ou category_browse (40%)
    → bounce (30%) — termina aqui
    → product_view (70%)
      → product_review_read (40%, opcional)
      → no_cart (65%) — termina aqui
      → add_to_cart (35%)
        → [85%] add_to_cart direto
        → [15%] remove_from_cart → add_to_cart (re-add)
        → cart_view
          → cart_abandon (45%) — termina aqui
          → checkout_start (55%)
            → checkout_abandon (20%) — termina aqui
            → order_placed (80%) → Kafka + PostgreSQL
              → session_end (completed)
                → review_submitted (40%, com delay 30–300s) → MinIO
```

Retorna uma tupla `(events[], purchase | None, review | None)`.

**Dispositivos:** mobile 55% | desktop 35% | tablet 10%

O evento `order_placed` é adicionado à lista `events` e vai para o Kafka como qualquer outro evento. Simultaneamente, os dados transacionais da encomenda são também escritos no PostgreSQL. Ou seja, o `order_placed` vai para **ambos os destinos**.

---

### `producer.py` — Clickstream para Kafka

Publica todos os eventos do funil no tópico `clickstream_events`, incluindo o `order_placed`. Usa o `session_id` como chave Kafka, o que garante que todos os eventos da mesma sessão vão para a mesma partição e chegam ordenados. Tem callback de delivery para logging de falhas.

---

### `db_writer.py` — Compras para PostgreSQL

O PostgreSQL tem um schema relacional com 5 tabelas: `customers`, `sellers`, `products`, `orders` e `order_items`. As tabelas de dimensão (`customers`, `sellers`, `products`) crescem organicamente — cada nova compra faz upsert apenas do cliente/seller/produto envolvido.

Para cada compra:
1. Insere/ignora o cliente em `customers`
2. Insere/ignora o seller em `sellers`
3. Insere/ignora o produto em `products`
4. Insere a encomenda em `orders`
5. Insere o item em `order_items`

Usa `ON CONFLICT DO NOTHING` em todas as inserções — idempotente. Usa `autocommit=False` com commit explícito após cada conjunto de inserções.

Tem também `noop_update_purchase()`, que executa `UPDATE orders SET state = state WHERE order_id = ?`. Isto não altera nenhum dado, mas faz o PostgreSQL gerar um evento no WAL, que o Debezium captura e publica no Kafka como operação `u` (update). Serve para simular um re-delivery do CDC — o Bronze recebe dois registos com o mesmo `order_id`, testando a deduplicação da Silver.

---

### `review_writer.py` — Reviews para MinIO

Escreve cada review como um ficheiro `.txt` no bucket `raw-reviews`, com o caminho:

```
raw-reviews/{YYYY-MM-DD}/{review_id}_{order_id}.txt
```

O ficheiro tem texto livre no seguinte formato:

```
Boa tarde, o meu nome é Ana Silva e venho partilhar a minha opinião
sobre um produto da categoria electronics.
<mensagem real do dataset Olist>
Dou 4 estrelas em 5.
```

O rating está em linguagem natural em uma de duas formas (50%/50%):
- `Dou 4 estrelas em 5.`
- `Classifico este produto com 4/5.`

A camada Silver extrai o rating com dual-regex para cobrir ambos os formatos.

Os IDs estruturados (`review_id`, `order_id`) estão **no nome do ficheiro**, não no conteúdo — a review em si é texto não estruturado.

`write_review_duplicate()` escreve o mesmo conteúdo com `_dup` no nome (`{review_id}_{order_id}_dup.txt`), simulando submissão dupla pelo utilizador. O `file_watcher` processa ambos os ficheiros para Bronze com o mesmo `review_id`, testando a deduplicação da Silver.

---

### `noise.py` — Injeção de ruído

Módulo que simula problemas reais de qualidade de dados. É aplicado em `main.py` **antes** de qualquer escrita. A intensidade do ruído é proporcional à natureza da fonte — reviews são texto humano (mais erros), orders vêm de uma BD estruturada (erros pontuais), clickstream é gerado por browsers (mínimo ruído).

| Fonte | Problema simulado | Probabilidade |
|-------|-------------------|---------------|
| Reviews | Rating inválido (0, 6, -1, "N/A") | 5% |
| Reviews | Campo obrigatório vazio (review_id, order_id, customer_id) | 4% |
| Reviews | Mensagem vazia ou só whitespace | 3% |
| Reviews | Corrupção de encoding UTF-8→Latin-1 (ex: "ã" → "Ã£") | 7% |
| Reviews | Submissão duplicada (ficheiro `_dup`) | 5% |
| Orders | Preço negativo ou zero | 4% |
| Orders | State com casing errado ("sp"), espaços (" SP ") ou typo ("SP0") | 6% |
| Orders | Categoria com espaços em volta | 5% |
| Orders | CDC re-delivery (noop UPDATE) | 4% |
| Clickstream | session_id nulo | 3% |
| Clickstream | Device inválido ("bot", "crawler", "tv") | 4% |
| Clickstream | event_type nulo | 2% |

---

### `config.py` — Configurações

Centraliza todos os endereços de serviços e parâmetros de simulação. Todos os valores são sobreponíveis por variáveis de ambiente, o que permite correr o simulador tanto localmente como dentro do Docker sem alterar código.

```
KAFKA_BOOTSTRAP     = localhost:29092
CLICKSTREAM_TOPIC   = clickstream_events

DB_HOST             = localhost
DB_PORT             = 5434
DB_NAME             = Amazon_Sales

MINIO_ENDPOINT      = localhost:9004
REVIEWS_BUCKET      = raw-reviews

SESSIONS_PER_SECOND = 0.5
DEVICES             = mobile(55%), desktop(35%), tablet(10%)

P_SEARCH            = 0.60
P_PRODUCT_VIEW      = 0.70
P_REVIEW_READ       = 0.40
P_ADD_TO_CART       = 0.35
P_REMOVE_FROM_CART  = 0.15
P_CART_ABANDON      = 0.45
P_ORDER_PLACED      = 0.80
P_REVIEW_SUBMIT     = 0.40

EVENT_DELAY_MIN     = 2s
EVENT_DELAY_MAX     = 15s
REVIEW_DELAY_MIN    = 30s
REVIEW_DELAY_MAX    = 300s
```

---

## Os três outputs e porquê cada destino

| Output | Destino | Justificação |
|--------|---------|--------------|
| **Clickstream** | Kafka | Alta frequência, sem schema fixo, requer streaming — Kafka é o padrão para event streaming |
| **Compras** | PostgreSQL | Dados estruturados, transacionais, com relações entre tabelas — BD relacional é o destino natural; o CDC via Debezium captura as mudanças pelo WAL automaticamente |
| **Reviews** | MinIO | Texto não estruturado, tamanho variável, sem schema — object storage é adequado; simula um sistema de ficheiros de um CRM real |

---

## Perguntas prováveis na defesa

**"O `order_placed` vai para o Kafka?"**
Sim. O `session.py` adiciona o `order_placed` à lista `events`, e o `producer.py` publica todos os eventos dessa lista sem filtro. O docstring do `producer.py` diz que é excluído, mas o código não implementa essa exclusão. O `order_placed` vai para **Kafka e PostgreSQL** em simultâneo.

**"O que é o WAL e porque é importante?"**
WAL (Write-Ahead Log) é o registo de todas as alterações que o PostgreSQL escreve antes de as aplicar. O Debezium lê este log e publica cada INSERT/UPDATE/DELETE no Kafka. Isto significa que a Data Engineering recebe as encomendas sem que o simulador precise de escrever diretamente para o Kafka — a captura é transparente.

**"Como é que a Silver sabe se uma review é duplicada?"**
O `review_writer` escreve dois ficheiros com o mesmo `review_id` mas nomes diferentes. O `file_watcher` processa ambos para Bronze. A Silver usa `MERGE INTO` sobre `review_id` — se já existe, não insere. É assim que a deduplicação é testada.

**"Porque é que as reviews têm delay?"**
Para simular comportamento humano real — após uma compra, o utilizador não escreve a review imediatamente. O delay é de 30 a 300 segundos. O timestamp da review reflete esse delay, mas o ficheiro é escrito imediatamente no MinIO.

**"O que é o noop update e para que serve?"**
É um `UPDATE orders SET state = state` — atualiza uma linha sem alterar nenhum dado. O PostgreSQL gera mesmo assim um evento no WAL, que o Debezium captura e publica no Kafka como operação `u`. Serve para simular at-least-once delivery do CDC: o Bronze recebe dois registos com o mesmo `order_id`, e a Silver tem de os deduplicar.

**"Porque é que o ruído das reviews é maior do que o do clickstream?"**
Porque as reviews são texto humano — mais suscetível a erros de escrita, encoding e submissões duplicadas. As orders vêm de uma BD estruturada, por isso o ruído é mais pontual. O clickstream é gerado por browsers, que são sistemas controlados, por isso tem o mínimo de ruído.
