# Responsabilidade deste job:
# 1. Ler arquivos da Landing Zone
# 2. Utilizar Databricks Auto Loader
# 3. Utilizar readStream
# 4. Persistir schema inferido
# 5. Utilizar checkpoint
# 6. Adicionar metadados técnicos
# 7. Gravar Bronze Delta em append-only
#
# IMPORTANTE:
# Este job NÃO acessa o MongoDB.
# O MongoDB -> Landing é responsabilidade do ingestion_job.py.

import json
import time
import uuid
import yaml

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, StructField, StructType, IntegerType, DoubleType, ArrayType,
)

# ============================================================
# CONFIGURAÇÃO
# ============================================================
# pipeline_config.yaml: catalog, schemas, paths e parâmetros do Auto Loader.
# collections.json: lista de coleções a processar (database, collection,
# modo_carga, campo_watermark, destino, projecao).

PIPELINE_CONFIG_PATH = "/Workspace/Users/lcspinheiro17@gmail.com/Projeto_Ingestao_Moderna_Pipeline_Mflix/config/pipeline_config.yaml"
COLLECTIONS_CONFIG_PATH = "/Workspace/Users/lcspinheiro17@gmail.com/Projeto_Ingestao_Moderna_Pipeline_Mflix/config/collections.json"

with open(PIPELINE_CONFIG_PATH, "r", encoding="utf-8") as f:
    PIPELINE = yaml.safe_load(f)["pipeline"]

with open(COLLECTIONS_CONFIG_PATH, "r", encoding="utf-8") as f:
    COLLECTIONS_CONFIG = json.load(f)

CATALOG = PIPELINE["catalog"]
BRONZE_SCHEMA = PIPELINE["bronze_schema"]
LANDING_BASE_PATH = PIPELINE["landing_base_path"]
CHECKPOINT_BASE_PATH = PIPELINE["checkpoint_base_path"]
SCHEMA_BASE_PATH = PIPELINE["schema_base_path"]
INFER_COLUMN_TYPES = bool(PIPELINE.get("infer_column_types", True))
SCHEMA_EVOLUTION_MODE = PIPELINE.get("schema_evolution_mode", "rescue")
AVAILABLE_NOW = bool(PIPELINE.get("available_now", True))

# ============================================================
# SCHEMAS EXPLÍCITOS POR COLLECTION
# ============================================================
# Em vez de depender só da inferência do Auto Loader, cada collection tem
# um contrato de schema formal. Campos fora do contrato (e não existentes
# no dict abaixo) vão parar em `_rescued_data` (schema_evolution_mode: rescue).
# `_id` é incluído em todos os schemas para preservar o `_source_id` depois.

_TIPOS = {
    "string": StringType(),
    "int": IntegerType(),
    "double": DoubleType(),
}


def _para_tipo_spark(valor):
    if isinstance(valor, dict):
        return _para_struct(valor)
    if valor.startswith("array<") and valor.endswith(">"):
        return ArrayType(_TIPOS[valor[6:-1]])
    return _TIPOS[valor]


def _para_struct(d: dict) -> StructType:
    return StructType([
        StructField(nome, _para_tipo_spark(tipo), True)
        for nome, tipo in d.items()
    ])


MOVIES_SCHEMA_DICT = {
    "_id": "string",
    "title": "string", "year": "int", "runtime": "int", "released": "string",
    "rated": "string", "plot": "string", "genres": "array<string>",
    "directors": "array<string>", "writers": "array<string>", "cast": "array<string>",
    "countries": "array<string>", "languages": "array<string>",
    "imdb": {"rating": "double", "votes": "int", "id": "int"},
    "tomatoes": {
        "viewer": {"rating": "double", "numReviews": "int", "meter": "int"},
        "critic": {"rating": "double", "numReviews": "int", "meter": "int"},
        "fresh": "int", "rotten": "int", "lastUpdated": "string",
    },
    "awards": {"wins": "int", "nominations": "int", "text": "string"},
    "lastupdated": "string", "num_mflix_comments": "int",
    "poster": "string", "type": "string", "_corrupt_record": "string",
}

COMMENTS_SCHEMA_DICT = {
    "_id": "string",
    "name": "string", "email": "string", "movie_id": "string",
    "text": "string", "date": "string", "_corrupt_record": "string",
}

USERS_SCHEMA_DICT = {
    "_id": "string",
    "name": "string", "email": "string", "_corrupt_record": "string",
    # password fica de fora do schema: é excluído via projeção {"password": 0} na leitura
}

THEATERS_SCHEMA_DICT = {
    "_id": "string",
    "theaterId": "int",
    "location": {
        "address": {"street1": "string", "city": "string", "state": "string", "zipcode": "string"},
        "geo": {"type": "string", "coordinates": "array<double>"},
    },
    "_corrupt_record": "string",
}

SESSIONS_SCHEMA_DICT = {
    "_id": "string",
    "user_id": "string", "_corrupt_record": "string",
    # jwt fica de fora do schema: é excluído via projeção {"jwt": 0} na leitura
}

EMBEDDED_MOVIES_SCHEMA_DICT = {
    "_id": "string",
    "title": "string", "year": "int", "plot": "string", "_corrupt_record": "string",
    # plot_embedding fica de fora: excluído via projeção {"plot_embedding": 0}
}

SCHEMAS_DICT_BY_COLLECTION = {
    "movies": MOVIES_SCHEMA_DICT,
    "comments": COMMENTS_SCHEMA_DICT,
    "users": USERS_SCHEMA_DICT,
    "theaters": THEATERS_SCHEMA_DICT,
    "sessions": SESSIONS_SCHEMA_DICT,
    "embedded_movies": EMBEDDED_MOVIES_SCHEMA_DICT,
}

SCHEMAS_BY_COLLECTION = {
    colecao: _para_struct(schema_dict)
    for colecao, schema_dict in SCHEMAS_DICT_BY_COLLECTION.items()
}

# ============================================================
# TABELA DE CONTROLE
# ============================================================

CONTROL_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.control_ingestion_log"


def create_control_table():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
            ingestion_id STRING,
            collection STRING,
            batch_id BIGINT,
            qtd_registros BIGINT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            status STRING,
            mensagem_erro STRING
        )
        USING DELTA
    """)


# ============================================================
# METADADOS BRONZE
# ============================================================

def add_bronze_metadata(df, collection_cfg):
    return (
        df
        .withColumn("_ingestion_id", F.expr("uuid()"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_path", F.lit("mongodb_atlas"))
        .withColumn("_source_collection", F.lit(collection_cfg["collection"]))
        .withColumn("_load_type", F.lit(collection_cfg["modo_carga"]))
        .withColumn("_ingestion_date", F.current_date())
        .withColumn(
            "_source_id",
            F.col("_id").cast("string") if "_id" in df.columns else F.lit(None).cast("string"),
        )
    )


# ============================================================
# BRONZE
# ============================================================

def load_to_bronze(collection_cfg):
    collection = collection_cfg["collection"]
    destino = collection_cfg["destino"]  # já vem como "bronze.<tabela>"

    landing_path = f"{LANDING_BASE_PATH.rstrip('/')}/{collection}"
    checkpoint_path = f"{CHECKPOINT_BASE_PATH.rstrip('/')}/{collection}"
    schema_path = f"{SCHEMA_BASE_PATH.rstrip('/')}/{collection}"

    # destino já contém o schema (ex.: "bronze.movies") -> só falta o catálogo
    bronze_table = f"{CATALOG}.{destino}"

    print("=" * 80)
    print(f"BRONZE: {collection}")
    print(f"Landing: {landing_path}")
    print(f"Schema: {schema_path}")
    print(f"Checkpoint: {checkpoint_path}")
    print(f"Tabela: {bronze_table}")
    print("=" * 80)

    # ========================================================
    # AUTO LOADER
    # ========================================================
    schema = SCHEMAS_BY_COLLECTION.get(collection)
    if schema is None:
        raise ValueError(
            f"Não existe schema explícito definido para a collection '{collection}' "
            f"em SCHEMAS_DICT_BY_COLLECTION."
        )

    df_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", str(INFER_COLUMN_TYPES).lower())
        .option("cloudFiles.schemaEvolutionMode", SCHEMA_EVOLUTION_MODE)
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.includeExistingFiles", "true")
        .schema(schema)
        .load(landing_path)
    )

    df_bronze = add_bronze_metadata(df_stream, collection_cfg)

    # ========================================================
    # WRITE STREAM
    # ========================================================
    query = (
        df_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_date")
        .trigger(availableNow=AVAILABLE_NOW)
        .toTable(bronze_table)
    )

    # `availableNow=True` deveria encerrar a query sozinha assim que
    # processar tudo que já existe na Landing. Em compute Serverless
    # (Spark Connect), tanto o awaitTermination() quanto checagens
    # repetidas de status (isActive/lastProgress) podem ficar pendurados
    # por instabilidade de comunicação client<->cluster. Por isso, em vez
    # de fazer polling, esperamos um tempo fixo (suficiente pro volume de
    # dados da Landing) e encerramos a query manualmente, sem depender de
    # nenhuma chamada de status no meio do caminho.
    TIMEOUT_SECONDS = 90
    print(f"  aguardando até {TIMEOUT_SECONDS}s para o processamento da collection...")
    time.sleep(TIMEOUT_SECONDS)
    try:
        query.stop()
    except Exception as exc:
        print(f"  aviso ao encerrar a query: {exc}")

    print(f"Bronze concluída: {bronze_table}")


# ============================================================
# EXECUÇÃO
# ============================================================

create_control_table()

for collection_cfg in COLLECTIONS_CONFIG:
    if not collection_cfg.get("enabled", True):
        continue
    load_to_bronze(collection_cfg)