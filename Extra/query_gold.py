"""
TrendMart — Consulta rápida às tabelas Gold via Trino.
Uso: python query_gold.py
"""

import trino

conn = trino.dbapi.connect(
    host="localhost",
    port=8085,
    user="admin",
    catalog="lake",
    schema="gold",
)

cur = conn.cursor()

query = "SELECT * FROM lake.gold.fact_sales LIMIT 100"

print(f"\nQuery: {query}\n")
cur.execute(query)

cols = [d[0] for d in cur.description]
rows = cur.fetchall()

print("\t".join(cols))
print("-" * 120)
for row in rows:
    print("\t".join(str(v) for v in row))

print(f"\n{len(rows)} linha(s) retornada(s).")
