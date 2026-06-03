# Bronze — Reviews

## O que é e para que serve

O Bronze Reviews é responsável por monitorizar o bucket `raw-reviews` no MinIO, detetar novos ficheiros `.txt` e armazená-los em Parquet no bucket `bronze`. Ao contrário do clickstream e das orders, não usa Kafka — usa polling direto ao MinIO.

---

## Ficheiro responsável

`data_engineering/ingestion/file_watcher.py`

---

## Fonte

- **Localização:** MinIO, bucket `raw-reviews`
- **Formato:** ficheiros `.txt` com texto livre
- **Path dos ficheiros:** `raw-reviews/{YYYY-MM-DD}/{review_id}_{order_id}.txt`
- **Duplicados:** `raw-reviews/{YYYY-MM-DD}/{review_id}_{order_id}_dup.txt`

O conteúdo dos ficheiros é texto não estruturado:
```
Boa tarde, o meu nome é Ana Silva e venho partilhar a minha opinião
sobre um produto da categoria electronics.
<mensagem real do dataset Olist>
Dou 4 estrelas em 5.
```

Os IDs estruturados (`review_id`, `order_id`) estão **no nome do ficheiro**, não no conteúdo.

---

## Como funciona

### 1. Estado local

O `file_watcher` mantém um ficheiro de estado `ingestion/.watcher_state.json` com os paths de todos os ficheiros já processados:

```python
processed = _load_state()   # carrega set de paths já vistos
```

Garante que nunca re-ingere o mesmo ficheiro, mesmo que o processo reinicie.

### 2. Polling a cada 30 segundos

```python
while True:
    paginator = minio.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket="raw-reviews")
    ...
    time.sleep(30)
```

Lista todos os objetos do bucket e verifica quais são novos (não estão no `processed`).

### 3. Processamento de cada ficheiro novo

Para cada ficheiro `.txt` não processado:
1. Faz download do conteúdo
2. Extrai `review_id` e `order_id` do nome do ficheiro com `_parse_review_file()`
3. Guarda o conteúdo inteiro em `raw_content` — sem parsear o texto
4. Adiciona `ingested_at`
5. Marca o path como processado

```python
filename = "abc123_def456.txt"
stem     = "abc123_def456"
review_id, _, order_id = stem.partition("_")   # "abc123", "def456"
```

Para ficheiros `_dup.txt`, o sufixo é removido antes do parse:
```python
stem = filename.removesuffix("_dup.txt").removesuffix(".txt")
```

### 4. Flush por batch

Ao contrário do `consumer.py`, o `file_watcher` não tem buffer com limite de tamanho. Processa todos os ficheiros novos de uma vez e faz um único flush por ciclo de polling.

Após o flush, guarda o estado atualizado no `watcher_state.json`.

---

## Armazenamento final

**Bucket:** `bronze`  
**Path:**
```
bronze/reviews/year=YYYY/month=MM/day=DD/batch_<ts>.parquet
```

Particionamento por **dia** (não por hora como o clickstream) — o volume de reviews é muito menor do que o de eventos de clickstream.

**Estrutura de colunas de cada ficheiro Parquet:**

| Campo | Tipo | Descrição |
|---|---|---|
| `file_path` | STRING | Path completo do ficheiro no MinIO |
| `review_id` | STRING | Extraído do nome do ficheiro |
| `order_id` | STRING | Extraído do nome do ficheiro |
| `raw_content` | STRING | Conteúdo completo do `.txt` sem parse |
| `ingested_at` | STRING | Timestamp de ingestão no Bronze |

---

## Ruído presente

O Bronze não filtra nada — os dados chegam com o ruído injetado pelo `noise.py`:

| Problema | Probabilidade |
|---|---|
| Rating inválido (0, 6, -1, "N/A") no texto | 5% |
| Campo obrigatório vazio (`review_id`, `order_id`) no nome do ficheiro | 4% |
| Mensagem vazia ou só whitespace | 3% |
| Corrupção de encoding UTF-8→Latin-1 (ex: "ã" → "Ã£") | 7% |
| Ficheiro duplicado (`_dup.txt`) com mesmo `review_id` | 5% |

Os duplicados chegam ao Bronze como dois registos com o mesmo `review_id` mas `file_path` diferente. A deduplicação é feita na Silver via `MERGE INTO` por `review_id`.
