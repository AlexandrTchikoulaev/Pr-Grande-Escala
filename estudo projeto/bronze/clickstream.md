# Bronze — Clickstream

## O que é e para que serve

O Bronze Clickstream é responsável por consumir os eventos de clickstream do Kafka e armazená-los em Parquet no MinIO, sem qualquer transformação. É a primeira camada de persistência dos dados de comportamento dos utilizadores.

---

## Ficheiro responsável

`data_engineering/ingestion/consumer.py`

---

## Fonte

- **Localização:** Kafka, tópico `clickstream_events`
- **Formato:** JSON por mensagem
- **Chave Kafka:** `session_id` — garante que todos os eventos da mesma sessão vão para a mesma partição e chegam ordenados

---

## Como funciona

### 1. Ligação ao Kafka

```python
consumer = Consumer({
    "bootstrap.servers":  "localhost:29092",
    "group.id":           "de_clickstream_consumer",
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": True,
})
consumer.subscribe(["clickstream_events"])
```

- **`group.id`** — se o processo reiniciar, o Kafka sabe onde ficou e continua do mesmo offset
- **`auto.offset.reset = earliest`** — na primeira execução começa do início do tópico, não perde mensagens
- **`enable.auto.commit = True`** — o Kafka confirma automaticamente os offsets lidos

### 2. Loop de leitura

O `poll(timeout=1.0)` bloqueia até 1 segundo à espera de uma mensagem. Devolve uma mensagem de cada vez:

- **Há mensagem** — regressa imediatamente com a mensagem
- **Não há mensagem** — espera 1 segundo e regressa com `None`
- **Erro** — ignora `PARTITION_EOF`, imprime os outros

### 3. Processamento de cada mensagem

Para cada mensagem recebida:
1. Deserializa o JSON
2. Adiciona `ingested_at` com o timestamp atual (Europe/Lisbon)
3. Converte `properties` de dicionário para string JSON — o Bronze não interpreta o conteúdo
4. Adiciona ao buffer

### 4. Buffer

Lista Python em memória que acumula os eventos processados:

```python
buffer: list[dict] = []
```

Cada elemento é um dicionário com os campos do evento já processado. Não é persistido — se o processo morrer, os dados no buffer perdem-se (mas o Kafka permite reler as mensagens não confirmadas).

### 5. Flush para Parquet

O buffer é descarregado quando atinge **500 registos** ou **30 segundos**, o que acontecer primeiro:

1. Constrói o path com o timestamp atual
2. Converte a lista de dicionários para uma tabela Arrow
3. Serializa para Parquet em memória (`BytesIO`) — nunca escreve em disco
4. Envia para o MinIO

---

## Armazenamento final

**Bucket:** `bronze`  
**Path:**
```
bronze/clickstream/year=YYYY/month=MM/day=DD/hour=HH/batch_<ts>.parquet
```

**Particionamento por hora** — permite ao Spark fazer partition pruning: ao processar a Silver, só lê as pastas das horas relevantes em vez de varrer todos os ficheiros.

**Estrutura de colunas de cada ficheiro Parquet:**

| Campo | Tipo | Descrição |
|---|---|---|
| `event_id` | STRING | ID único do evento |
| `session_id` | STRING | ID da sessão |
| `user_id` | STRING | ID do utilizador |
| `event_type` | STRING | Tipo de evento (session_start, product_view, ...) |
| `timestamp` | STRING | Timestamp do evento (gerado pelo simulador) |
| `device` | STRING | Dispositivo (mobile, desktop, tablet, ou ruído) |
| `properties` | STRING | JSON serializado com campos específicos do evento |
| `ingested_at` | STRING | Timestamp de ingestão no Bronze |

---

## Ruído presente

O Bronze não filtra nada — os dados chegam com o ruído injetado pelo `noise.py`:

| Problema | Probabilidade |
|---|---|
| `session_id` nulo | 3% |
| `device` inválido ("bot", "crawler", "tv") | 4% |
| `event_type` nulo | 2% |

Estes problemas são tratados na Silver.

---

## Health check

A cada 5 segundos verifica se o consumer ainda tem partições Kafka atribuídas. Se ficou 2 minutos sem partições, termina com exit code 1 para o Docker reiniciar o processo. O ficheiro `/tmp/healthy` é tocado enquanto está saudável.
