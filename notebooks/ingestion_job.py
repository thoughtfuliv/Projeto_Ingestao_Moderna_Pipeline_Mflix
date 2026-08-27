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
import time
import uuid
import bson
import yaml
from pymongo import MongoClient
from pyspark.sql import functions as F

# COMMAND ----------

############################################
##  CONFIGURAÇÃO (pipeline_config.yaml)   ##
############################################
## Mesmo arquivo de config já usado pelo bronze_job.py, pra manter
## catalog / bronze_schema / landing_base_path como fonte única de verdade.

PIPELINE_CONFIG_PATH = "/Workspace/Users/lcspinheiro17@gmail.com/Projeto_Ingestao_Moderna_Pipeline_Mflix/config/pipeline_config.yaml"
COLLECTIONS_CONFIG_PATH = "/Workspace/Users/lcspinheiro17@gmail.com/Projeto_Ingestao_Moderna_Pipeline_Mflix/config/collections.json"

with open(PIPELINE_CONFIG_PATH, "r", encoding="utf-8") as f:
    PIPELINE = yaml.safe_load(f)["pipeline"]

with open(COLLECTIONS_CONFIG_PATH, "r", encoding="utf-8") as f:
    COLLECTIONS_CONFIG = json.load(f)  # lista de dicts: database, collection, modo_carga,
                                        # campo_watermark, destino, projecao

CATALOG = PIPELINE["catalog"]
BRONZE_SCHEMA = PIPELINE["bronze_schema"]
LANDING_SCHEMA = PIPELINE["landing_schema"]
CHECKPOINTS_SCHEMA = PIPELINE["checkpoints_schema"]
SCHEMAS_SCHEMA = PIPELINE["schemas_schema"]
VOLUME_NAME = PIPELINE["volume_name"]
LANDING_BASE_PATH = PIPELINE["landing_base_path"]
BATCH_SIZE = int(PIPELINE.get("batch_size", 5000))
MAX_RETRIES = int(PIPELINE.get("max_retries", 3))

CONTROL_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.control_landing_log"
WATERMARK_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.control_watermark"

# COMMAND ----------

############################################
##   TABELAS DE CONTROLE (lógica antiga,  ##
##   já validada em produção)             ##
############################################
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{LANDING_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{LANDING_SCHEMA}.{VOLUME_NAME}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{CHECKPOINTS_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{CHECKPOINTS_SCHEMA}.{VOLUME_NAME}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMAS_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMAS_SCHEMA}.{VOLUME_NAME}")


spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
  ingestion_id STRING, collection STRING, load_type STRING,
  watermark_inicial STRING, watermark_final STRING,
  qtd_exportada BIGINT,
  start_time TIMESTAMP, end_time TIMESTAMP, duracao_seg DOUBLE,
  status STRING, mensagem_erro STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
  collection STRING, watermark_value STRING, updated_at TIMESTAMP
) USING DELTA
""")


def ler_watermark(collection: str):
    df = spark.table(WATERMARK_TABLE).filter(F.col("collection") == collection)
    if df.count() == 0:
        return None
    return df.orderBy(F.col("updated_at").desc()).first()["watermark_value"]


def gravar_watermark(collection: str, valor: str):
    spark.sql(f"DELETE FROM {WATERMARK_TABLE} WHERE collection = '{collection}'")
    spark.createDataFrame(
        [(collection, valor, datetime.datetime.now(datetime.timezone.utc))],
        "collection STRING, watermark_value STRING, updated_at TIMESTAMP",
    ).write.format("delta").mode("append").saveAsTable(WATERMARK_TABLE)


def montar_filtro(cfg: dict, watermark_atual):
    campo = cfg.get("campo_watermark")
    if cfg["modo_carga"] != "incremental" or not campo or not watermark_atual:
        return {}
    if cfg["collection"] == "comments":
        return {campo: {"$gt": {"$date": watermark_atual}}}
    return {campo: {"$gt": watermark_atual}}  # movies.lastupdated: string, comparação lexicográfica


def log_execucao(row: dict):
    schema = """
      ingestion_id STRING, collection STRING, load_type STRING,
      watermark_inicial STRING, watermark_final STRING,
      qtd_exportada BIGINT,
      start_time TIMESTAMP, end_time TIMESTAMP, duracao_seg DOUBLE,
      status STRING, mensagem_erro STRING
    """
    spark.createDataFrame([row], schema=schema).write.format("delta").mode("append").saveAsTable(CONTROL_TABLE)

# COMMAND ----------

############################################
##  CONEXÃO MONGODB (mesma que já          ##
##  funcionava na versão anterior)        ##
############################################

MONGODB_URI = dbutils.secrets.get(scope="conn-db", key="cnn-mongodb-sampleflix")


def _encode(o):
    """Converte tipos BSON para valores serializáveis em JSON."""
    if isinstance(o, bson.ObjectId):
        return str(o)
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    if isinstance(o, bson.Decimal128):
        return str(o)
    if isinstance(o, bytes):
        return o.hex()
    return str(o)


def get_nested_value(document, field):
    """Busca campo simples ou nested usando dot notation."""
    value = document
    for part in field.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value

# COMMAND ----------

############################################
##  EXPORTAÇÃO: MONGODB -> LANDING ZONE   ##
############################################
## Único ponto de entrada, parametrizado por cfg (igual à versão antiga) —
## mas em vez de gravar na Bronze, grava JSON Lines na Landing.
## A Bronze é responsabilidade exclusiva do bronze_job.py (Auto Loader).

def exportar_para_landing(cfg: dict):
    ingestion_id = str(uuid.uuid4())
    start = datetime.datetime.now(datetime.timezone.utc)

    database = cfg["database"]
    collection = cfg["collection"]
    projecao = cfg.get("projecao") or {}

    wm_inicial = ler_watermark(collection) if cfg["modo_carga"] == "incremental" else None
    status, msg, total, wm_final = "SUCCESS", "", 0, wm_inicial

    client = MongoClient(
        MONGODB_URI,
        serverSelectionTimeoutMS=15_000,
        socketTimeoutMS=300_000,
        appName="databricks-mongodb-connector",
    )

    landing_path = f"{LANDING_BASE_PATH.rstrip('/')}/{collection}"
    dbutils.fs.mkdirs(landing_path)

    try:
        filtro = montar_filtro(cfg, wm_inicial)

        print("=" * 80)
        print(f"Collection: {collection}")
        print(f"Modo: {cfg['modo_carga']}")
        print(f"Watermark inicial: {wm_inicial}")
        print(f"Filtro: {filtro}")
        print("=" * 80)

        tentativa = 0
        lote, lote_num = [], 0

        while True:
            try:
                cursor = client[database][collection].find(
                    filter=filtro, projection=projecao, batch_size=BATCH_SIZE
                )
                for doc in cursor:
                    # Atualiza candidato a watermark a partir do documento bruto
                    if cfg.get("campo_watermark"):
                        valor = get_nested_value(doc, cfg["campo_watermark"])
                        if valor is not None:
                            valor = (
                                valor.isoformat()
                                if isinstance(valor, (datetime.datetime, datetime.date))
                                else str(valor)
                            )
                            if wm_final is None or valor > wm_final:
                                wm_final = valor

                    lote.append(json.dumps(doc, default=_encode, ensure_ascii=False))

                    if len(lote) >= BATCH_SIZE:
                        file_path = f"{landing_path}/{collection}__{ingestion_id}__{lote_num:06d}.json"
                        dbutils.fs.put(file_path, "\n".join(lote), overwrite=False)
                        print(f"Landing criada: {file_path} | registros: {len(lote)}")
                        total += len(lote)
                        lote, lote_num = [], lote_num + 1

                cursor.close()
                break
            except Exception as exc:
                tentativa += 1
                if tentativa > MAX_RETRIES:
                    raise
                espera = 2 ** tentativa
                print(
                    f"Erro na leitura de {collection}: {exc}. "
                    f"Tentativa {tentativa}/{MAX_RETRIES}. Aguardando {espera}s..."
                )
                time.sleep(espera)

        # Último lote (menor que BATCH_SIZE)
        if lote:
            file_path = f"{landing_path}/{collection}__{ingestion_id}__{lote_num:06d}.json"
            dbutils.fs.put(file_path, "\n".join(lote), overwrite=False)
            print(f"Landing criada: {file_path} | registros: {len(lote)}")
            total += len(lote)

        # Persiste o watermark só depois de confirmar que os arquivos foram gravados
        if cfg["modo_carga"] == "incremental" and total > 0 and wm_final:
            gravar_watermark(collection, wm_final)

    except Exception as e:
        status, msg = "FAILED", str(e)[:500]
    finally:
        client.close()

    end = datetime.datetime.now(datetime.timezone.utc)
    log_execucao({
        "ingestion_id": ingestion_id,
        "collection": collection,
        "load_type": cfg["modo_carga"],
        "watermark_inicial": wm_inicial,
        "watermark_final": wm_final,
        "qtd_exportada": total,
        "start_time": start,
        "end_time": end,
        "duracao_seg": (end - start).total_seconds(),
        "status": status,
        "mensagem_erro": msg,
    })

    print("=" * 80)
    print(f"Exportação finalizada: {collection}")
    print(f"Total exportado: {total}")
    print(f"Watermark final: {wm_final}")
    print(f"Status: {status} {msg}")
    print("=" * 80)

# COMMAND ----------

for collection_cfg in COLLECTIONS_CONFIG:
    if not collection_cfg.get("enabled", True):
        continue
    exportar_para_landing(collection_cfg)

display(spark.table(CONTROL_TABLE).orderBy(F.col("start_time").desc()))