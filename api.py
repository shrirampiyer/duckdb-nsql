import db_config
from sqlalchemy import create_engine, text

from llama_index.llms.ollama import Ollama
from llama_index.core import SQLDatabase
from llama_index.core.query_engine import NLSQLTableQueryEngine
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core import Settings

import psycopg2

def get_connection():
    conn = psycopg2.connect(
        dbname=db_config.db_name,
        user=db_config.db_user,
        password=db_config.db_password,
        host=db_config.db_host,
        port=db_config.db_port
    )
    return conn

def main():
  llm = Ollama(model="duckdb-nsql", request_timeout=30.0)
  print("Selected Model :: ", llm.model)
  print("=====================")

Settings.embed_model = HuggingFaceEmbedding(
        model_name="BAAI/bge-small-en-v1.5"
    )

engine = get_connection()
db_tables = ["members"]

sql_database = SQLDatabase(engine, include_tables=db_tables)
query_engine = NLSQLTableQueryEngine(sql_database=sql_database,
                                     tables=db_tables,
                                     llm=llm)

query_str = "How many members older than 65 years ?"
response = query_engine.query(query_str)
