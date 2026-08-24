# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %pip install pymongo
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

############################################
##          BIBLIOTECAS PYTHON            ##
############################################

import datetime
import json
import os
import time
import uuid
import bson
from pymongo import MongoClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, StructField, StructType, IntegerType, DoubleType, ArrayType
)

############################################
##   PASSO UM: INGESTÃO PADRÃO DO MONGODB ##
############################################
## Alterações em relação à versão anterior (R2):
##   - read() agora lê em lotes (batch_size) e faz union incremental, em vez de
##     `docs = [... for d in cursor]` (que trazia a coleção inteira pro driver).
##   - retry com backoff exponencial em falha de rede/timeout.

class MongoReader:

    SCHEMA = StructType([
        StructField("_id", StringType(), True),
        StructField("body", StringType(), True),
    ])

    def __init__(self, database: str = "sample_mflix", max_retries: int = 3):
        self.spark = SparkSession.builder.getOrCreate()
        self.database = database
        self.max_retries = max_retries
        self.uri = dbutils.secrets.get(scope="conn-db", key="cnn-mongodb-sampleflix")
        self.client = MongoClient(self.uri, serverSelectionTimeoutMS=15_000,
                                  socketTimeoutMS=300_000, appName="databricks-mongodb-connector")

    @staticmethod
    def _encode(o):
        if isinstance(o, bson.ObjectId):
            return str(o)
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()
        if isinstance(o, bson.Decimal128):
            return str(o)
        if isinstance(o, bytes):
            return o.hex()
        return str(o)

    def collections(self) -> list[str]:
        return sorted(self.client[self.database].list_collection_names())

    def count(self, colecao: str, filtro: dict | None = None) -> int:
        return self.client[self.database][colecao].count_documents(filtro or {})

    def _iter_lotes(self, colecao: str, filtro: dict, projecao: dict | None, batch_size: int):
        """Gera lotes de (_id, body_json) sem acumular a coleção inteira em memória (R2)."""
        tentativa = 0
        while True:
            try:
                cursor = self.client[self.database][colecao].find(
                    filter=filtro or {}, projection=projecao, batch_size=batch_size
                )
                lote = []
                for d in cursor:
                    lote.append((str(d.get("_id", "")),
                                 json.dumps(d, default=self._encode, ensure_ascii=False)))
                    if len(lote) >= batch_size:
                        yield lote
                        lote = []
                if lote:
                    yield lote
                return
            except Exception:
                tentativa += 1
                if tentativa > self.max_retries:
                    raise
                time.sleep(2 ** tentativa)  # retry com backoff exponencial (R2)

    def read(self, colecao: str, filtro: dict | None = None, projecao: dict | None = None,
             limite: int | None = None, batch_size: int = 5_000,
             infer: bool = False, sample_size: int = 1_000) -> DataFrame:
        df_final = None
        lidos = 0
        for lote in self._iter_lotes(colecao, filtro or {}, projecao, batch_size):
            if limite:
                lote = lote[: max(0, limite - lidos)]
            df_lote = self.spark.createDataFrame(lote, schema=self.SCHEMA)
            df_final = df_lote if df_final is None else df_final.unionByName(df_lote)
            lidos += len(lote)
            if limite and lidos >= limite:
                break

        if df_final is None:
            df_final = self.spark.createDataFrame([], schema=self.SCHEMA)

        return self.expand(df_final, sample_size) if infer else df_final

    def infer_schema(self, df: DataFrame, sample_size: int = 1_000) -> str:
        return df.limit(sample_size).select(F.expr("schema_of_json_agg(body)")).first()[0]

    def expand(self, df: DataFrame, schema=None, sample_size: int = 1_000) -> DataFrame:
        if schema is None:
            schema = self.infer_schema(df, sample_size)  # fallback sem contrato formal
        opcoes = {"mode": "PERMISSIVE"}
        campos = schema.fieldNames() if hasattr(schema, "fieldNames") else []
        if "_corrupt_record" in campos:
            opcoes["columnNameOfCorruptRecord"] = "_corrupt_record"
        # mantém "body" (JSON bruto) ao lado do parse -> vira o rescue de verdade (R7)
        return (
            df.select("_id", "body", F.from_json("body", schema, opcoes).alias("doc"))
              .select("_id", "body", "doc.*")
        )

    def close(self):
        self.client.close()


##################################################
## PASSO DOIS: STRUCTTYPE DE TODAS AS COLEÇÕES  ##
##################################################

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
    "name": "string", "email": "string", "movie_id": "string",
    "text": "string", "date": "string", "_corrupt_record": "string",
}

USERS_SCHEMA_DICT = {
    "name": "string", "email": "string", "_corrupt_record": "string",
    # password fica de fora do schema: é excluído via projeção {"password": 0} na leitura
}

THEATERS_SCHEMA_DICT = {
    "theaterId": "int",
    "location": {
        "address": {"street1": "string", "city": "string", "state": "string", "zipcode": "string"},
        "geo": {"type": "string", "coordinates": "array<double>"},
    },
    "_corrupt_record": "string",
}

SESSIONS_SCHEMA_DICT = {
    "user_id": "string", "_corrupt_record": "string",
    # jwt fica de fora do schema: é excluído via projeção {"jwt": 0} na leitura
}

EMBEDDED_MOVIES_SCHEMA_DICT = {
    "title": "string", "year": "int", "plot": "string", "_corrupt_record": "string",
    # plot_embedding fica de fora: excluído via projeção {"plot_embedding": 0}
}

SCHEMAS_DICT_BY_COLLECTION = {
    "movies": MOVIES_SCHEMA_DICT,          # <- estava faltando, causava o KeyError
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

# COMMAND ----------

############################################
##  PASSO TRÊS: CONFIGURAÇÃO EXTERNALIZADA ##
############################################
## R1: nada de database/collection/modo_carga/watermark/destino hardcoded no corpo do
## código — tudo vem de config/collections.json, lido em runtime.

dbutils.widgets.text("config_path", "/Workspace/Repos/<voce>/Projeto_Ingestao_Moderna_Pipeline_Mflix/config/collections.json")
dbutils.widgets.text("catalog", "meu_catalog")
dbutils.widgets.text("limiar_divergencia_pct", "1.0")

CONFIG_PATH = dbutils.widgets.get("config_path")
CATALOG = dbutils.widgets.get("catalog")
LIMIAR_DIVERGENCIA = float(dbutils.widgets.get("limiar_divergencia_pct"))
CONTROL_TABLE = f"{CATALOG}.bronze.control_ingestion_log"
WATERMARK_TABLE = f"{CATALOG}.bronze.control_watermark"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    COLLECTIONS_CONFIG = json.load(f)  # lista de dicts: database, collection, modo_carga,
                                        # campo_watermark, destino, projecao

# COMMAND ----------

############################################
##      PASSO QUATRO: CONTROL (R1/R5)      ##
############################################

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
  _ingestion_id STRING, collection STRING, load_type STRING,
  watermark_inicial STRING, watermark_final STRING,
  qtd_lida_origem BIGINT, qtd_gravada_destino BIGINT,
  start_time TIMESTAMP, end_time TIMESTAMP, duracao_seg DOUBLE,
  status STRING, mensagem_erro STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
  collection STRING, watermark_value STRING, updated_at TIMESTAMP
) USING DELTA
""")


def ler_watermark(collection: str) -> str | None:
    df = spark.table(WATERMARK_TABLE).filter(F.col("collection") == collection)
    return None if df.count() == 0 else df.orderBy(F.col("updated_at").desc()).first()["watermark_value"]


def gravar_watermark(collection: str, valor: str):
    spark.sql(f"DELETE FROM {WATERMARK_TABLE} WHERE collection = '{collection}'")
    spark.createDataFrame([(collection, valor, datetime.datetime.now(datetime.timezone.utc))],
        "collection STRING, watermark_value STRING, updated_at TIMESTAMP"
    ).write.format("delta").mode("append").saveAsTable(WATERMARK_TABLE)


CONTROL_LOG_SCHEMA = """
  _ingestion_id STRING, collection STRING, load_type STRING,
  watermark_inicial STRING, watermark_final STRING,
  qtd_lida_origem BIGINT, qtd_gravada_destino BIGINT,
  start_time TIMESTAMP, end_time TIMESTAMP, duracao_seg DOUBLE,
  status STRING, mensagem_erro STRING
"""

def log_execucao(row: dict):
    spark.createDataFrame([row], schema=CONTROL_LOG_SCHEMA).write.format("delta").mode("append").saveAsTable(CONTROL_TABLE)


def montar_filtro(cfg: dict, watermark_atual: str | None) -> dict:
    campo = cfg.get("campo_watermark")
    if cfg["modo_carga"] != "incremental" or not campo or not watermark_atual:
        return {}
    if cfg["collection"] == "comments":
        return {campo: {"$gt": {"$date": watermark_atual}}}
    return {campo: {"$gt": watermark_atual}}  # movies.lastupdated: string, comparação lexicográfica


# COMMAND ----------

############################################
##       PASSO CINCO: LOAD (R1/R3/R6)      ##
############################################

def enriquecer_bronze(df, cfg: dict, ingestion_id: str):
    df = (
        df.withColumn("_ingestion_id", F.lit(ingestion_id))
          .withColumn("_ingestion_timestamp", F.current_timestamp())
          .withColumn("_source_path", F.lit("mongodb_atlas"))
          .withColumn("_load_type", F.lit(cfg["modo_carga"]))
          .withColumn("_ingestion_date", F.current_date())
          .withColumn("_source_id", F.col("_id").cast("string"))
    )
    # rescue de verdade (R7): guarda o JSON bruto só quando o parse falhou
    # (_corrupt_record não nulo), nunca descarta o registro.
    if "_corrupt_record" in df.columns:
        df = df.withColumn(
            "_rescued_data",
            F.when(F.col("_corrupt_record").isNotNull(), F.col("body")).otherwise(F.lit(None).cast("string"))
        )
    elif "_rescued_data" not in df.columns:
        df = df.withColumn("_rescued_data", F.lit(None).cast("string"))
    if "body" in df.columns:
        df = df.drop("body")  # já foi copiado pra _rescued_data quando necessário
    return df


def carregar_bronze(df, cfg: dict):
    destino = f"{CATALOG}.{cfg['destino']}"

    # controle de paralelismo/partições no destino (R2) — evita small files
    df = df.repartition(4)

    if cfg["modo_carga"] == "full":
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true").option("mergeSchema", "true")
           .partitionBy("_ingestion_date").saveAsTable(destino))
    else:
        if not spark.catalog.tableExists(destino):
            (df.write.format("delta").mode("append").option("mergeSchema", "true")
               .partitionBy("_ingestion_date").saveAsTable(destino))
        else:
            df.createOrReplaceTempView("_stage")
            spark.sql(f"""
                MERGE INTO {destino} AS tgt USING _stage AS src
                ON tgt._source_id = src._source_id
                WHEN NOT MATCHED THEN INSERT *
            """)  # idempotente: rodar de novo não duplica, porque casa por _source_id (R3)


def reconciliar(cfg: dict, ingestion_id: str, qtd_origem: int, qtd_destino_run: int):
    if qtd_origem == 0:
        return "SUCCESS", "sem novidades" if cfg["modo_carga"] == "incremental" else "coleção vazia"
    div_pct = abs(qtd_origem - qtd_destino_run) / qtd_origem * 100
    destino = f"{CATALOG}.{cfg['destino']}"
    # nulos e duplicados restritos ao lote desta execução (_ingestion_id), não ao dia inteiro
    lote = spark.table(destino).filter(F.col("_ingestion_id") == ingestion_id)
    nulos = lote.filter(F.col("_source_id").isNull()).count()
    dup = lote.groupBy("_source_id").count().filter("count > 1").count()
    if div_pct > LIMIAR_DIVERGENCIA or nulos > 0 or dup > 0:
        return "PARTIAL", f"divergencia={div_pct:.2f}%, nulos={nulos}, duplicados={dup}"
    return "SUCCESS", ""

# COMMAND ----------

############################################
## PASSO SEIS: COMPONENTE ÚNICO (R1)       ##
############################################
## Um único ponto de entrada que recebe database, collection, modo_carga,
## campo_watermark e destino (via cfg, vindo do JSON) — mesma função pra
## todas as coleções, nenhum bloco copiado por coleção.

def executar_ingestao(cfg: dict):
    ingestion_id = str(uuid.uuid4())
    start = datetime.datetime.now(datetime.timezone.utc)
    wm_inicial = ler_watermark(cfg["collection"]) if cfg["modo_carga"] == "incremental" else None
    status, msg, qtd_origem, qtd_destino, wm_final = "SUCCESS", "", 0, 0, wm_inicial

    reader = MongoReader(database=cfg["database"])
    try:
        filtro = montar_filtro(cfg, wm_inicial)
        df_raw = reader.read(cfg["collection"], filtro=filtro, projecao=cfg.get("projecao"))
        df = reader.expand(df_raw, schema=SCHEMAS_BY_COLLECTION[cfg["collection"]])
        df = enriquecer_bronze(df, cfg, ingestion_id)
        qtd_origem = df.count()

        carregar_bronze(df, cfg)

        if cfg["modo_carga"] == "incremental" and qtd_origem > 0 and cfg.get("campo_watermark"):
            campo = cfg["campo_watermark"]
            if campo in df.columns:
                wm_final = str(df.agg(F.max(campo)).first()[0])
                gravar_watermark(cfg["collection"], wm_final)

        destino = f"{CATALOG}.{cfg['destino']}"
        qtd_destino = spark.table(destino).filter(F.col("_ingestion_id") == ingestion_id).count()
        status, msg = reconciliar(cfg, ingestion_id, qtd_origem, qtd_destino)
    except Exception as e:
        status, msg = "FAILED", str(e)[:500]
    finally:
        reader.close()

    end = datetime.datetime.now(datetime.timezone.utc)
    log_execucao({
        "_ingestion_id": ingestion_id, "collection": cfg["collection"], "load_type": cfg["modo_carga"],
        "watermark_inicial": wm_inicial, "watermark_final": wm_final,
        "qtd_lida_origem": qtd_origem, "qtd_gravada_destino": qtd_destino,
        "start_time": start, "end_time": end, "duracao_seg": (end - start).total_seconds(),
        "status": status, "mensagem_erro": msg,
    })
    return status

# COMMAND ----------

for cfg in COLLECTIONS_CONFIG:
    executar_ingestao(cfg)

display(spark.table(CONTROL_TABLE).orderBy(F.col("start_time").desc()))