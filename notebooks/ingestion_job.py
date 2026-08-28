# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %pip install pymongo
# MAGIC dbutils.library.restartPython()
# COMMAND ----------
# Databricks notebook source
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
# Este job NÃO grava na Bronze.
# A Bronze é responsabilidade exclusiva do bronze_job.py.
#
# ============================================================


# ============================================================
# IMPORTS
# ============================================================

import datetime as dt
import json
import time
import uuid

from dataclasses import dataclass
from typing import Any, Dict, Optional

import bson
import yaml

from pymongo import MongoClient
from pyspark.sql.functions import desc as spark_desc


# ============================================================
# CONFIGURAÇÃO
# ============================================================

# Caminho do projeto no Databricks
CONFIG_DIR = (
    "/Workspace/Users/lcspinheiro17@gmail.com/Projeto_Ingestao_Moderna_Pipeline_Mflix/config"
)


# ------------------------------------------------------------
# Pipeline configuration
# ------------------------------------------------------------

with open(
    f"{CONFIG_DIR}/pipeline_config.yaml",
    "r",
    encoding="utf-8"
) as f:
    CONFIG = yaml.safe_load(f)


# ------------------------------------------------------------
# Collections configuration
# ------------------------------------------------------------

with open(
    f"{CONFIG_DIR}/collections.json",
    "r",
    encoding="utf-8"
) as f:
    COLLECTIONS = json.load(f)


PIPELINE = CONFIG["pipeline"]


CATALOG = PIPELINE["catalog"]
LANDING_SCHEMA = PIPELINE["landing_schema"]
BRONZE_SCHEMA = PIPELINE["bronze_schema"]
VOLUME_NAME = PIPELINE["volume_name"]


# ------------------------------------------------------------
# Parâmetros técnicos
# ------------------------------------------------------------

BATCH_SIZE = int(
    PIPELINE.get("batch_size", 5000)
)

MAX_RETRIES = int(
    PIPELINE.get("max_retries", 3)
)


# ============================================================
# TABELA DE CONTROLE
# ============================================================

CONTROL_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.control_ingestion_log"
)


# ============================================================
# LANDING ZONE
# ============================================================

# IMPORTANTE:
# Não colocar "/landing/sample_mflix" aqui.
#
# O Volume já é:
#
# /Volumes/meu_catalog/landing/mflix/
#
# As coleções serão criadas diretamente dentro dele.

LANDING_BASE_PATH = (
    f"/Volumes/"
    f"{CATALOG}/"
    f"{LANDING_SCHEMA}/"
    f"{VOLUME_NAME}"
)


# ============================================================
# SETUP UNITY CATALOG
# ============================================================

def setup_unity_catalog_objects():
    """
    Cria somente a infraestrutura necessária para o projeto.

    Estrutura:

    Catalog
      ├── landing
      │    └── Volume mflix
      │
      └── bronze
           └── tabelas Bronze

    Não são criados schemas separados para:
    - checkpoints
    - schema inference
    """

    spark.sql(
        f"CREATE CATALOG IF NOT EXISTS {CATALOG}"
    )

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS "
        f"{CATALOG}.{LANDING_SCHEMA}"
    )

    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS "
        f"{CATALOG}.{LANDING_SCHEMA}.{VOLUME_NAME}"
    )

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS "
        f"{CATALOG}.{BRONZE_SCHEMA}"
    )


# ============================================================
# CONTROLE DE EXECUÇÕES
# ============================================================

CONTROL_SCHEMA = (
    "_ingestion_id STRING, "
    "collection STRING, "
    "load_type STRING, "
    "watermark_inicial STRING, "
    "watermark_final STRING, "
    "qtd_lida_origem BIGINT, "
    "qtd_gravada_destino BIGINT, "
    "start_time TIMESTAMP, "
    "end_time TIMESTAMP, "
    "duracao_seg DOUBLE, "
    "status STRING, "
    "mensagem_erro STRING"
)


def create_control_table():

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS "
        f"{CATALOG}.{BRONZE_SCHEMA}"
    )

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


# ============================================================
# RESULTADO DA EXTRAÇÃO
# ============================================================

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


# ============================================================
# LOG DA EXTRAÇÃO
# ============================================================

def log_control_extraction(
    result: ExtractionResult
):

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

    row.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(CONTROL_TABLE)


    # Passa o ingestion_id para o bronze_job.py
    # quando os notebooks estiverem sendo executados
    # como tasks do mesmo Databricks Job.

    try:

        dbutils.jobs.taskValues.set(
            key=f"ingestion_id__{result.collection}",
            value=result.ingestion_id,
        )

    except Exception:
        pass


# ============================================================
# SERIALIZAÇÃO
# ============================================================

def encode_value(value):
    """
    Converte tipos BSON para valores serializáveis em JSON.
    """

    if isinstance(value, bson.ObjectId):
        return str(value)

    if isinstance(
        value,
        (dt.datetime, dt.date)
    ):
        return value.isoformat()

    if isinstance(
        value,
        bson.Decimal128
    ):
        return str(value)

    if isinstance(value, bytes):
        return value.hex()

    return str(value)


# ============================================================
# WATERMARK
# ============================================================

def parse_watermark(value):

    if not value:
        return None

    value = value.replace(
        "Z",
        "+00:00"
    )

    parsed = dt.datetime.fromisoformat(
        value
    )

    if parsed.tzinfo is None:
        parsed = parsed.replace(
            tzinfo=dt.timezone.utc
        )

    return parsed


def get_nested_value(
    document,
    field_name
):
    """
    Busca campo simples ou nested
    usando dot notation.
    """

    value = document

    for part in field_name.split("."):

        if not isinstance(
            value,
            dict
        ):
            return None

        value = value.get(part)

    return value


def get_last_watermark(collection):

    """
    Busca o último watermark_final registrado
    com sucesso na tabela de controle.
    """

    try:

        if not spark.catalog.tableExists(
            CONTROL_TABLE
        ):
            return None


        row = (
            spark.table(CONTROL_TABLE)
            .filter(
                f"collection = '{collection}' "
                f"AND load_type = 'incremental' "
                f"AND status IN "
                f"('SUCCESS', 'PARTIAL')"
            )
            .orderBy(
                spark_desc("end_time")
            )
            .first()
        )


        return (
            row["watermark_final"]
            if row
            else None
        )


    except Exception:

        return None


# ============================================================
# FILTRO MONGODB
# ============================================================

def build_filter(
    collection_cfg,
    watermark
):

    """
    Full:
        {}

    Incremental:
        campo_watermark > último watermark
    """

    if (
        collection_cfg["modo_carga"]
        != "incremental"
    ):
        return {}


    campo = collection_cfg.get(
        "campo_watermark"
    )


    if not campo or not watermark:
        return {}


    # comments.date normalmente é Date
    # no MongoDB.

    if (
        collection_cfg["collection"]
        == "comments"
    ):

        return {
            campo: {
                "$gt": parse_watermark(
                    watermark
                )
            }
        }


    # Campos armazenados como string
    # ex.: movies.lastupdated

    return {
        campo: {
            "$gt": watermark
        }
    }


# ============================================================
# MONGODB -> LANDING
# ============================================================

class MongoLandingExtractor:

    """
    Componente genérico de extração.

    Recebe as configurações da coleção
    e não possui lógica específica hardcoded
    para cada coleção.
    """

    def __init__(
        self,
        client: MongoClient,
        batch_size: int,
        max_retries: int
    ):

        self.client = client
        self.batch_size = batch_size
        self.max_retries = max_retries


    # --------------------------------------------------------
    # PREPARAÇÃO DA LANDING
    # --------------------------------------------------------

    def _prepare_landing_dir(
        self,
        collection_cfg: Dict[str, Any],
        collection_path: str
    ):

        # FULL:
        # limpa toda a coleção antes de exportar
        # o novo snapshot.

        if (
            collection_cfg["modo_carga"]
            == "full"
        ):

            dbutils.fs.rm(
                collection_path,
                recurse=True
            )


        # Cria a pasta da execução.

        dbutils.fs.mkdirs(
            collection_path
        )


    # --------------------------------------------------------
    # EXTRAÇÃO
    # --------------------------------------------------------

    def extract(
        self,
        collection_cfg: Dict[str, Any]
    ) -> ExtractionResult:

        database = collection_cfg[
            "database"
        ]

        collection = collection_cfg[
            "collection"
        ]

        projection = (
            collection_cfg.get(
                "projecao"
            )
            or {}
        )

        load_type = collection_cfg[
            "modo_carga"
        ]


        # ----------------------------------------------------
        # ID DA EXECUÇÃO
        # ----------------------------------------------------

        ingestion_id = str(
            uuid.uuid4()
        )


        start_time = dt.datetime.utcnow()


        # ----------------------------------------------------
        # WATERMARK
        # ----------------------------------------------------

        watermark_initial = (

            get_last_watermark(
                collection
            )

            if load_type == "incremental"

            else None
        )


        mongo_filter = build_filter(
            collection_cfg,
            watermark_initial
        )


        # ----------------------------------------------------
        # DATA DA INGESTÃO
        # ----------------------------------------------------

        ingestion_date = (
            dt.datetime.utcnow()
            .strftime("%Y-%m-%d")
        )


        # ----------------------------------------------------
        # CAMINHO DA COLEÇÃO
        # ----------------------------------------------------

        collection_path = (
            f"{LANDING_BASE_PATH.rstrip('/')}/"
            f"{collection}"
        )


        # ----------------------------------------------------
        # CAMINHO FINAL DA EXECUÇÃO
        # ----------------------------------------------------

        # É AQUI que organizamos por data.
        #
        # Resultado:
        #
        # /comments/
        #     _ingestion_date=2026-08-28/

        execution_path = (
            f"{collection_path}/"
            f"_ingestion_date={ingestion_date}"
        )


        # ----------------------------------------------------
        # PREPARA LANDING
        # ----------------------------------------------------

        self._prepare_landing_dir(
            collection_cfg,
            collection_path
        )

        # Depois do FULL limpar a coleção,
        # recria a pasta da data.

        dbutils.fs.mkdirs(
            execution_path
        )


        # ----------------------------------------------------
        # CONTADORES
        # ----------------------------------------------------

        total = 0

        batch = []

        batch_number = 0

        watermark_final = (
            watermark_initial
        )

        status = "EXTRACTED"

        mensagem_erro = None


        # ----------------------------------------------------
        # LOG
        # ----------------------------------------------------

        print("=" * 80)

        print(
            f"Collection: {collection}"
        )

        print(
            f"Modo: {load_type}"
        )

        print(
            f"Data da ingestão: "
            f"{ingestion_date}"
        )

        print(
            f"Landing: "
            f"{execution_path}"
        )

        print(
            f"Watermark inicial: "
            f"{watermark_initial}"
        )

        print(
            f"Filtro: "
            f"{mongo_filter}"
        )

        print("=" * 80)


        # ----------------------------------------------------
        # EXTRAÇÃO
        # ----------------------------------------------------

        tentativa = 0


        try:

            while True:

                try:

                    # ----------------------------------------
                    # Cursor paginado
                    # ----------------------------------------

                    cursor = (
                        self.client[
                            database
                        ][
                            collection
                        ].find(
                            filter=mongo_filter,
                            projection=projection,
                            batch_size=self.batch_size,
                        )
                    )


                    # ----------------------------------------
                    # Processa documento por documento
                    # ----------------------------------------

                    for document in cursor:

                        watermark_final = (
                            self._update_watermark_candidate(
                                watermark_final,
                                document,
                                collection_cfg.get(
                                    "campo_watermark"
                                ),
                            )
                        )


                        # ------------------------------------
                        # JSON Lines
                        # ------------------------------------

                        body = json.dumps(
                            document,
                            default=encode_value,
                            ensure_ascii=False
                        )


                        batch.append(
                            body
                        )


                        # ------------------------------------
                        # Grava lote
                        # ------------------------------------

                        if (
                            len(batch)
                            >= self.batch_size
                        ):

                            self._flush_batch(
                                execution_path,
                                collection,
                                ingestion_id,
                                batch_number,
                                batch,
                            )


                            total += len(
                                batch
                            )


                            batch = []

                            batch_number += 1


                    cursor.close()

                    break


                except Exception as exc:

                    tentativa += 1


                    if (
                        tentativa
                        > self.max_retries
                    ):
                        raise


                    espera = (
                        2 ** tentativa
                    )


                    print(
                        f"Erro na leitura de "
                        f"{collection}: {exc}. "
                        f"Tentativa "
                        f"{tentativa}/"
                        f"{self.max_retries}. "
                        f"Aguardando "
                        f"{espera}s..."
                    )


                    time.sleep(
                        espera
                    )


            # ------------------------------------------------
            # Último lote
            # ------------------------------------------------

            if batch:

                self._flush_batch(
                    execution_path,
                    collection,
                    ingestion_id,
                    batch_number,
                    batch,
                )


                total += len(
                    batch
                )


        except Exception as exc:

            status = "FAILED"

            mensagem_erro = str(
                exc
            )

            print(
                f"[ERRO] Falha ao "
                f"extrair {collection}: "
                f"{exc}"
            )


        # ----------------------------------------------------
        # FINALIZAÇÃO
        # ----------------------------------------------------

        end_time = dt.datetime.utcnow()


        print("=" * 80)

        print(
            f"Exportação finalizada: "
            f"{collection}"
        )

        print(
            f"Landing final: "
            f"{execution_path}"
        )

        print(
            f"Total exportado: "
            f"{total}"
        )

        print(
            f"Watermark final: "
            f"{watermark_final}"
        )

        print(
            f"Status: "
            f"{status}"
        )

        print("=" * 80)


        return ExtractionResult(

            ingestion_id=ingestion_id,

            collection=collection,

            load_type=load_type,

            watermark_inicial=(
                watermark_initial
            ),

            watermark_final=(
                watermark_final
            ),

            qtd_lida_origem=total,

            start_time=start_time,

            end_time=end_time,

            status=status,

            mensagem_erro=(
                mensagem_erro
            ),
        )


    # --------------------------------------------------------
    # ATUALIZA WATERMARK
    # --------------------------------------------------------

    @staticmethod
    def _update_watermark_candidate(
        current,
        document,
        field_name
    ):

        if not field_name:
            return current


        value = get_nested_value(
            document,
            field_name
        )


        if value is None:
            return current


        if isinstance(
            value,
            (dt.datetime, dt.date)
        ):

            value = value.isoformat()


        value = str(value)


        if (
            current is None
            or value > current
        ):

            return value


        return current


    # --------------------------------------------------------
    # GRAVA ARQUIVO JSONL
    # --------------------------------------------------------

    @staticmethod
    def _flush_batch(
        execution_path,
        collection,
        ingestion_id,
        batch_number,
        batch
    ):

        # Nome do arquivo:
        #
        # comments__UUID__000000.json

        file_path = (
            f"{execution_path}/"
            f"{collection}__"
            f"{ingestion_id}__"
            f"{batch_number:06d}.json"
        )


        # JSON Lines:
        # uma linha = um documento MongoDB.

        dbutils.fs.put(
            file_path,
            "\n".join(batch),
            overwrite=False
        )


        print(
            f"Wrote "
            f"{len(''.join(batch))} bytes. "
            f"Landing criada: "
            f"{file_path} | "
            f"registros: "
            f"{len(batch)}"
        )


# ============================================================
# EXECUÇÃO
# ============================================================

setup_unity_catalog_objects()

create_control_table()


# ------------------------------------------------------------
# Cliente MongoDB único para o job inteiro
# ------------------------------------------------------------

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


# ------------------------------------------------------------
# Extractor
# ------------------------------------------------------------

extractor = MongoLandingExtractor(

    client=MONGO_CLIENT,

    batch_size=BATCH_SIZE,

    max_retries=MAX_RETRIES,
)


# ------------------------------------------------------------
# Processamento das coleções
# ------------------------------------------------------------

try:

    for collection_cfg in COLLECTIONS:

        if not collection_cfg.get(
            "enabled",
            True
        ):
            continue


        result = extractor.extract(
            collection_cfg
        )


        log_control_extraction(
            result
        )


finally:

    MONGO_CLIENT.close()