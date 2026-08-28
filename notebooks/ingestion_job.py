# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %pip install pymongo
# MAGIC dbutils.library.restartPython()
# COMMAND ----------
# ============================================================
# INGESTION JOB — MongoDB Atlas -> Landing Zone
# ============================================================
# Responsabilidade deste job:
# 1. Ler dados do MongoDB (paginado, com projection e retry/backoff)
# 2. Aplicar carga full ou incremental, usando watermark persistida
# 3. Exportar documentos como JSON Lines para a Landing Zone
# 4. Registrar o resultado da extração na tabela de controle
#
# IMPORTANTE:
# Este job NÃO grava na Bronze. A Bronze é responsabilidade exclusiva do bronze_job.py.
#
# Boas práticas de uso de recursos aplicadas (R2 — mínimo 4 exigidas):
#   1. Leitura paginada: cursor do PyMongo com batch_size configurável
#   2. Projection pushdown: só os campos definidos em pipeline_config.yaml
#   3. Reuso de conexão: um único MongoClient (com pool) para o job inteiro
#   4. Retry com backoff exponencial em falha de leitura do Mongo
#   5. Nunca materializamos a coleção inteira em memória (sem list(cursor))
#
# Idempotência (R3):
#   - Carga FULL: antes de reexportar, a pasta da coleção na Landing é
#     limpa. Assim cada execução representa o snapshot atual, e não um
#     acúmulo de exportações anteriores (isso também é o que fazia a
#     Bronze demorar cada vez mais a cada execução).
#   - Carga INCREMENTAL: o filtro usa o watermark persistido na tabela
#     de controle, então documentos já extraídos não são lidos de novo.
# ============================================================

import datetime as dt
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import bson
import yaml
from pymongo import MongoClient
from pyspark.sql.functions import desc as spark_desc

# ============================================================
# CONFIGURAÇÃO
# ============================================================
# Infraestrutura (catalog, schemas, volume, parâmetros técnicos)
# fica em pipeline_config.yaml. As coleções a processar ficam
# separadas em collections.json, para trocar/adicionar coleções
# sem tocar na configuração de infraestrutura.
CONFIG_DIR = "/Workspace/Repos/<usuario>/Projeto_Ingestao_Moderna_Pipeline_Mflix/config"

with open(f"{CONFIG_DIR}/pipeline_config.yaml", "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

with open(f"{CONFIG_DIR}/collections.json", "r", encoding="utf-8") as f:
    COLLECTIONS = json.load(f)["collections"]

PIPELINE = CONFIG["pipeline"]

CATALOG = PIPELINE["catalog"]
LANDING_SCHEMA = PIPELINE["landing_schema"]
BRONZE_SCHEMA = PIPELINE["bronze_schema"]
CHECKPOINTS_SCHEMA = PIPELINE["checkpoints_schema"]
SCHEMAS_SCHEMA = PIPELINE["schemas_schema"]
VOLUME_NAME = PIPELINE["volume_name"]

BATCH_SIZE = int(PIPELINE.get("batch_size", 5000))
MAX_RETRIES = int(PIPELINE.get("max_retries", 3))

CONTROL_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.control_ingestion_log"

# Landing Zone dentro de um Volume gerenciado pelo Unity Catalog —
# funciona em qualquer workspace/cloud, sem path físico hardcoded.
LANDING_BASE_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{VOLUME_NAME}/landing/sample_mflix"


# ============================================================
# SETUP DE INFRAESTRUTURA (catalog / schemas / volumes)
# ============================================================
def setup_unity_catalog_objects():
    """
    Cria toda a infraestrutura do Unity Catalog necessária, de forma
    idempotente (IF NOT EXISTS). Assim a pipeline roda em qualquer
    workspace Databricks com Unity Catalog habilitado, sem setup
    manual prévio.
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{LANDING_SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{LANDING_SCHEMA}.{VOLUME_NAME}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{CHECKPOINTS_SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{CHECKPOINTS_SCHEMA}.{VOLUME_NAME}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMAS_SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMAS_SCHEMA}.{VOLUME_NAME}")

CONTROL_SCHEMA = (
    "_ingestion_id STRING, collection STRING, load_type STRING, "
    "watermark_inicial STRING, watermark_final STRING, "
    "qtd_lida_origem BIGINT, qtd_gravada_destino BIGINT, "
    "start_time TIMESTAMP, end_time TIMESTAMP, duracao_seg DOUBLE, "
    "status STRING, mensagem_erro STRING"
)

# ============================================================
# MONGODB — conexão única, reutilizada por todas as coleções (R2.3)
# ============================================================
MONGODB_URI = dbutils.secrets.get(
    scope="conn-db",
    key="cnn-mongodb-sampleflix",
)

MONGO_CLIENT = MongoClient(
    MONGODB_URI,
    serverSelectionTimeoutMS=15000,
    socketTimeoutMS=300000,
    appName="mflix-modern-ingestion",
    maxPoolSize=20,
)


# ============================================================
# CONTROLE DE EXECUÇÕES (R5)
# ============================================================
def create_control_table():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
            _ingestion_id STRING,
            collection STRING,
            load_type STRING,
            watermark_inicial STRING,
            watermark_final STRING,
            qtd_lida_origem BIGINT,
            qtd_gravada_destino BIGINT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duracao_seg DOUBLE,
            status STRING,
            mensagem_erro STRING
        )
        USING DELTA
        """
    )


@dataclass
class ExtractionResult:
    ingestion_id: str
    collection: str
    load_type: str
    watermark_inicial: Optional[str]
    watermark_final: Optional[str]
    qtd_lida_origem: int
    start_time: dt.datetime
    end_time: dt.datetime
    status: str
    mensagem_erro: Optional[str] = None


def log_control_extraction(result: ExtractionResult):
    """
    Insere a linha inicial da execução (fase de extração), com
    status 'EXTRACTED'. O bronze_job.py completa depois essa mesma
    linha (via MERGE por _ingestion_id) com qtd_gravada_destino e
    o status final (SUCCESS | PARTIAL | FAILED), após reconciliar.
    """
    row = spark.createDataFrame(
        [
            (
                result.ingestion_id,
                result.collection,
                result.load_type,
                result.watermark_inicial,
                result.watermark_final,
                result.qtd_lida_origem,
                None,
                result.start_time,
                result.end_time,
                None,
                result.status,
                result.mensagem_erro,
            )
        ],
        schema=CONTROL_SCHEMA,
    )
    row.write.format("delta").mode("append").saveAsTable(CONTROL_TABLE)

    # Repassa o _ingestion_id para o bronze_job.py, se os dois jobs
    # estiverem orquestrados como tasks do mesmo Databricks Job.
    # Se o notebook for rodado de forma avulsa, o bronze_job.py cai
    # no fallback de ler a última linha 'EXTRACTED' desta coleção.
    try:
        dbutils.jobs.taskValues.set(
            key=f"ingestion_id__{result.collection}",
            value=result.ingestion_id,
        )
    except Exception:
        pass


# ============================================================
# HELPERS DE SERIALIZAÇÃO / WATERMARK
# ============================================================
def encode_value(value):
    """Converte tipos BSON para valores serializáveis em JSON."""
    if isinstance(value, bson.ObjectId):
        return str(value)
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, bson.Decimal128):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def parse_watermark(value):
    """Converte watermark ISO para datetime quando necessário."""
    if not value:
        return None
    value = value.replace("Z", "+00:00")
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def get_nested_value(document, field_name):
    """Busca campo simples ou nested usando dot notation."""
    value = document
    for part in field_name.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def get_last_watermark(collection):
    """
    Busca o último watermark_final registrado com sucesso na tabela
    de controle para esta coleção. Retorna None se ainda não houver.
    """
    try:
        if not spark.catalog.tableExists(CONTROL_TABLE):
            return None
        row = (
            spark.table(CONTROL_TABLE)
            .filter(
                f"collection = '{collection}' "
                f"AND load_type = 'incremental' "
                f"AND status IN ('SUCCESS', 'PARTIAL')"
            )
            .orderBy(spark_desc("end_time"))
            .first()
        )
        return row["watermark_final"] if row else None
    except Exception:
        return None


def build_filter(collection_cfg, watermark):
    """
    Monta o filtro incremental.
      Full:        {}
      Incremental: campo_watermark > último watermark
    """
    if collection_cfg["modo_carga"] != "incremental":
        return {}

    campo = collection_cfg.get("campo_watermark")
    if not campo or not watermark:
        return {}

    # comments.date normalmente é armazenado como Date no MongoDB.
    if collection_cfg["collection"] == "comments":
        return {campo: {"$gt": parse_watermark(watermark)}}

    # Para campos armazenados como string (ex.: movies.lastupdated).
    return {campo: {"$gt": watermark}}


# ============================================================
# EXTRAÇÃO (extract + load na Landing)
# ============================================================
class MongoLandingExtractor:
    """
    Componente genérico de extração (R1): recebe database, collection,
    modo_carga, campo_watermark e destino via collection_cfg, e não tem
    nada hardcoded específico de uma coleção.
    """

    def __init__(self, client: MongoClient, batch_size: int, max_retries: int):
        self.client = client
        self.batch_size = batch_size
        self.max_retries = max_retries

    def _prepare_landing_dir(self, collection_cfg: Dict[str, Any], collection_path: str):
        if collection_cfg["modo_carga"] == "full":
            # Idempotência da carga full: o snapshot exportado agora
            # substitui o anterior, em vez de se somar a ele.
            dbutils.fs.rm(collection_path, recurse=True)
        dbutils.fs.mkdirs(collection_path)

    def extract(self, collection_cfg: Dict[str, Any]) -> ExtractionResult:
        database = collection_cfg["database"]
        collection = collection_cfg["collection"]
        projection = collection_cfg.get("projecao") or {}
        load_type = collection_cfg["modo_carga"]

        ingestion_id = str(uuid.uuid4())
        start_time = dt.datetime.utcnow()

        watermark_initial = (
            get_last_watermark(collection) if load_type == "incremental" else None
        )
        mongo_filter = build_filter(collection_cfg, watermark_initial)

        collection_path = f"{LANDING_BASE_PATH.rstrip('/')}/{collection}"
        self._prepare_landing_dir(collection_cfg, collection_path)

        total = 0
        batch = []
        batch_number = 0
        watermark_final = watermark_initial
        status = "EXTRACTED"
        mensagem_erro = None

        print("=" * 80)
        print(f"Collection: {collection}")
        print(f"Modo: {load_type}")
        print(f"Watermark inicial: {watermark_initial}")
        print(f"Filtro: {mongo_filter}")
        print("=" * 80)

        tentativa = 0
        try:
            while True:
                try:
                    # Leitura paginada via cursor (R2.1) + projection
                    # pushdown (R2.2). Nunca convertemos o cursor em
                    # lista nem usamos collect()/toPandas() (R2.4).
                    cursor = self.client[database][collection].find(
                        filter=mongo_filter,
                        projection=projection,
                        batch_size=self.batch_size,
                    )
                    for document in cursor:
                        watermark_final = self._update_watermark_candidate(
                            watermark_final,
                            document,
                            collection_cfg.get("campo_watermark"),
                        )

                        body = json.dumps(document, default=encode_value, ensure_ascii=False)
                        batch.append(body)

                        if len(batch) >= self.batch_size:
                            self._flush_batch(collection_path, collection, ingestion_id, batch_number, batch)
                            total += len(batch)
                            batch = []
                            batch_number += 1

                    cursor.close()
                    break
                except Exception as exc:
                    tentativa += 1
                    if tentativa > self.max_retries:
                        raise
                    espera = 2 ** tentativa
                    print(
                        f"Erro na leitura de {collection}: {exc}. "
                        f"Tentativa {tentativa}/{self.max_retries}. "
                        f"Aguardando {espera}s..."
                    )
                    time.sleep(espera)

            if batch:
                self._flush_batch(collection_path, collection, ingestion_id, batch_number, batch)
                total += len(batch)

        except Exception as exc:
            status = "FAILED"
            mensagem_erro = str(exc)
            print(f"[ERRO] Falha ao extrair {collection}: {exc}")

        end_time = dt.datetime.utcnow()

        print("=" * 80)
        print(f"Exportação finalizada: {collection}")
        print(f"Total exportado: {total}")
        print(f"Watermark final: {watermark_final}")
        print(f"Status: {status}")
        print("=" * 80)

        return ExtractionResult(
            ingestion_id=ingestion_id,
            collection=collection,
            load_type=load_type,
            watermark_inicial=watermark_initial,
            watermark_final=watermark_final,
            qtd_lida_origem=total,
            start_time=start_time,
            end_time=end_time,
            status=status,
            mensagem_erro=mensagem_erro,
        )

    @staticmethod
    def _update_watermark_candidate(current, document, field_name):
        if not field_name:
            return current
        value = get_nested_value(document, field_name)
        if value is None:
            return current
        if isinstance(value, (dt.datetime, dt.date)):
            value = value.isoformat()
        value = str(value)
        if current is None or value > current:
            return value
        return current

    @staticmethod
    def _flush_batch(collection_path, collection, ingestion_id, batch_number, batch):
        file_path = (
            f"{collection_path}/{collection}__{ingestion_id}__{batch_number:06d}.json"
        )
        # JSON Lines: uma linha = um documento MongoDB.
        dbutils.fs.put(file_path, "\n".join(batch), overwrite=False)
        print(f"Landing criada: {file_path} | registros: {len(batch)}")


# ============================================================
# EXECUÇÃO
# ============================================================
setup_unity_catalog_objects()
create_control_table()

extractor = MongoLandingExtractor(
    client=MONGO_CLIENT,
    batch_size=BATCH_SIZE,
    max_retries=MAX_RETRIES,
)

try:
    for collection_cfg in COLLECTIONS:
        if not collection_cfg.get("enabled", True):
            continue
        result = extractor.extract(collection_cfg)
        log_control_extraction(result)
finally:
    MONGO_CLIENT.close()