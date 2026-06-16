"""
RAG catalog query — answers natural-language questions about the data catalog.

Usage (from project root):
  python rag/query.py "which table has customer emails?"
  python rag/query.py "where is lifetime value calculated?"

Requirements:
  pip install openai psycopg2-binary sqlalchemy
  OPENAI_API_KEY must be set in the environment or .env
  catalog_embeddings must be populated by the rag_index Airflow DAG
"""
from __future__ import annotations

import os
import sys

WAREHOUSE_DSN = "postgresql+psycopg2://warehouse:warehouse@localhost:5433/warehouse"
TOP_K = 5


def embed_query(client, question: str) -> list[float]:
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=[question],
    )
    return response.data[0].embedding


def retrieve(engine, vec: list[float], k: int = TOP_K) -> list[dict]:
    import sqlalchemy

    vec_str = "[" + ",".join(str(x) for x in vec) + "]"
    sql = sqlalchemy.text("""
        SELECT source, model_name, column_name, description,
               1 - (embedding <=> :vec::vector) AS similarity
        FROM catalog_embeddings
        ORDER BY embedding <=> :vec::vector
        LIMIT :k
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"vec": vec_str, "k": k}).fetchall()
    return [dict(r._mapping) for r in rows]


def generate_answer(client, question: str, context: list[dict]) -> str:
    context_text = "\n".join(
        f"- [{r['source']}] {r['model_name']}"
        + (f".{r['column_name']}" if r["column_name"] else "")
        + f": {r['description']}"
        for r in context
    )
    messages = [
        {"role": "system",
         "content": (
             "You are a data catalog assistant. Answer questions about the "
             "data warehouse using only the catalog entries provided. "
             "Be concise and cite the model or column name."
         )},
        {"role": "user",
         "content": f"Question: {question}\n\nCatalog context:\n{context_text}"},
    ]
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        max_tokens=300,
    )
    return response.choices[0].message.content


def main() -> None:
    question = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""
    if not question:
        print("Usage: python rag/query.py <question>")
        sys.exit(1)

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        print("Error: OPENAI_API_KEY is not set.")
        sys.exit(1)

    import sqlalchemy
    from openai import OpenAI

    client = OpenAI(api_key=api_key)
    engine = sqlalchemy.create_engine(WAREHOUSE_DSN)

    print(f"\nQuestion: {question}")
    print("─" * 60)

    vec = embed_query(client, question)
    hits = retrieve(engine, vec)

    if not hits:
        print("No results found. Run the rag_index DAG first.")
        sys.exit(1)

    print("\nTop matches:")
    for h in hits:
        label = f"{h['model_name']}.{h['column_name']}" if h["column_name"] else h["model_name"]
        print(f"  [{h['similarity']:.3f}] {label} — {h['description'][:80]}")

    print("\nAnswer:")
    print(generate_answer(client, question, hits))


if __name__ == "__main__":
    main()
