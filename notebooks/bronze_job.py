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

import datetime as dt
import uuid

import yaml
from pyspark.sql import functions as F


# ============================================================
# CONFIGURAÇÃO
# ============================================================

CONFIG_PATH = "/Workspace/Repos/lcspinheiro17@gmail.com/Projeto_Ingestao_Moderna_Pipeline_Mflix/config/pipeline_config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

PIPELINE = CONFIG["pipeline"]

CATALOG = PIPELINE["catalog"]
BRONZE_SCHEMA = PIPELINE["bronze_schema"]

LANDING_BASE_PATH = PIPELINE["landing_base_path"]
CHECKPOINT_BASE_PATH = PIPELINE["checkpoint_base_path"]
SCHEMA_BASE_PATH = PIPELINE["schema_base_path"]

INFER_COLUMN_TYPES = bool(
    PIPELINE.get("infer_column_types", True)
)

SCHEMA_EVOLUTION_MODE = PIPELINE.get(
    "schema_evolution_mode",
    "rescue",
)

AVAILABLE_NOW = bool(
    PIPELINE.get("available_now", True)
)


# ============================================================
# TABELA DE CONTROLE
# ============================================================

CONTROL_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.control_ingestion_log"
)


def create_control_table():

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS "
        f"{CATALOG}.{BRONZE_SCHEMA}"
    )

    spark.sql(
        f"""
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
        """
    )


# ============================================================
# METADADOS BRONZE
# ============================================================

def add_bronze_metadata(df, collection_cfg):

    return (
        df

        # Identificador da ingestão
        .withColumn(
            "_ingestion_id",
            F.expr("uuid()"),
        )

        # Momento em que o registro entrou na Bronze
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp(),
        )

        # Arquivo que originou o registro
        .withColumn(
            "_source_path",
            F.input_file_name(),
        )

        # Collection MongoDB
        .withColumn(
            "_source_collection",
            F.lit(collection_cfg["collection"]),
        )

        # Tipo da carga
        .withColumn(
            "_load_type",
            F.lit(collection_cfg["modo_carga"]),
        )

        # Data técnica para particionamento
        .withColumn(
            "_ingestion_date",
            F.current_date(),
        )

        # ID original MongoDB
        .withColumn(
            "_source_id",
            (
                F.col("_id").cast("string")
                if "_id" in df.columns
                else F.lit(None).cast("string")
            ),
        )
    )


# ============================================================
# BRONZE
# ============================================================

def load_to_bronze(collection_cfg):

    collection = collection_cfg["collection"]
    destination = collection_cfg["destino"]

    landing_path = (
        f"{LANDING_BASE_PATH.rstrip('/')}/"
        f"{collection}"
    )

    checkpoint_path = (
        f"{CHECKPOINT_BASE_PATH.rstrip('/')}/"
        f"{collection}"
    )

    schema_path = (
        f"{SCHEMA_BASE_PATH.rstrip('/')}/"
        f"{collection}"
    )

    bronze_table = (
        f"{CATALOG}.{BRONZE_SCHEMA}.{destination}"
    )

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

    df_stream = (
        spark.readStream

        # Databricks Auto Loader
        .format("cloudFiles")

        # Arquivos da Landing são JSON Lines
        .option(
            "cloudFiles.format",
            "json",
        )

        # ====================================================
        # SCHEMA INFERENCE PERSISTIDA
        # ====================================================

        .option(
            "cloudFiles.schemaLocation",
            schema_path,
        )

        .option(
            "cloudFiles.inferColumnTypes",
            str(INFER_COLUMN_TYPES).lower(),
        )

        # ====================================================
        # SCHEMA EVOLUTION
        # ====================================================

        .option(
            "cloudFiles.schemaEvolutionMode",
            SCHEMA_EVOLUTION_MODE,
        )

        # Guarda campos inesperados
        .option(
            "cloudFiles.rescuedDataColumn",
            "_rescued_data",
        )

        # Processa também arquivos que já estavam na Landing
        # quando o stream foi iniciado.
        .option(
            "cloudFiles.includeExistingFiles",
            "true",
        )

        .load(landing_path)
    )

    # ========================================================
    # METADADOS
    # ========================================================

    df_bronze = add_bronze_metadata(
        df_stream,
        collection_cfg,
    )

    # ========================================================
    # WRITE STREAM
    # ========================================================

    query = (
        df_bronze.writeStream

        # Bronze append-only
        .format("delta")

        .outputMode("append")

        # ====================================================
        # CHECKPOINT
        # ====================================================

        .option(
            "checkpointLocation",
            checkpoint_path,
        )

        # Permite evolução de schema na tabela Delta
        .option(
            "mergeSchema",
            "true",
        )

        # Particionamento técnico
        .partitionBy(
            "_ingestion_date"
        )

        # ====================================================
        # AVAILABLE NOW
        # ====================================================

        # Processa os arquivos disponíveis e encerra.
        .trigger(
            availableNow=True
        )

        .toTable(
            bronze_table
        )
    )

    query.awaitTermination()

    print(
        f"Bronze concluída: {bronze_table}"
    )


# ============================================================
# EXECUÇÃO
# ============================================================

create_control_table()

for collection_cfg in CONFIG["collections"]:

    if not collection_cfg.get("enabled", True):
        continue

    load_to_bronze(collection_cfg)
