# Databricks notebook source
# ============================================================
# BRONZE JOB — Landing Zone -> Bronze (Delta Lake)
# ============================================================
# Responsabilidade deste job:
# 1. Ler arquivos da Landing Zone
# 2. Gravar Bronze append-only, com metadados de linhagem (R4)
# 3. Tratar schema drift preservando registros não convertidos (R7)
# 4. Reconciliar origem x destino e fechar a tabela de controle (R5, R8)
#
# IMPORTANTE:
# Este job NÃO acessa o MongoDB.
# O MongoDB -> Landing é responsabilidade do ingestion_job.py.
#
# Estratégia de idempotência por modo de carga (R3):
#   - INCREMENTAL (comments, movies): Auto Loader em streaming, append,
#     com checkpoint persistido dentro da própria Landing. O checkpoint
#     garante que arquivos já processados não sejam reprocessados.
#   - FULL (users, theaters): a Landing é limpa e reexportada por
#     inteiro a cada execução (ver ingestion_job.py), então a Bronze
#     usa "dynamic partition overwrite": só a partição
#     _ingestion_date do dia é substituída, sem duplicar e sem
#     apagar o histórico de dias anteriores.
#
# Estrutura física da Bronze (R6):
#   A tabela {catalog}.{bronze_schema}.<collection> é gravada com
#   partitionBy("_ingestion_date"), então o storage gerenciado pelo
#   Unity Catalog fica organizado como:
#     .../<collection>/_ingestion_date=YYYY-MM-DD/part-....parquet
#   Não usamos um path de nuvem fixo (abfss://, s3://, etc.) de
#   propósito: isso é o que permite rodar em qualquer workspace
#   Databricks sem editar paths por cloud provider.
# ============================================================

import datetime as dt
import json

import yaml
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# ============================================================
# CONFIGURAÇÃO
# ============================================================
# Infraestrutura (catalog, schemas, volume, parâmetros técnicos)
# fica em pipeline_config.yaml. As coleções a processar ficam
# separadas em collections.json.
CONFIG_DIR = "/Workspace/Repos/lcspinheiro17@gmail.com/Projeto_Ingestao_Moderna_Pipeline_Mflix/config"

with open(f"{CONFIG_DIR}/pipeline_config.yaml", "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

with open(f"{CONFIG_DIR}/collections.json", "r", encoding="utf-8") as f:
    COLLECTIONS = json.load(f)["collections"]

PIPELINE = CONFIG["pipeline"]

CATALOG = PIPELINE["catalog"]
LANDING_SCHEMA = PIPELINE["landing_schema"]
BRONZE_SCHEMA = PIPELINE["bronze_schema"]
VOLUME_NAME = PIPELINE["volume_name"]

INFER_COLUMN_TYPES = bool(PIPELINE.get("infer_column_types", True))
SCHEMA_EVOLUTION_MODE = PIPELINE.get("schema_evolution_mode", "rescue")
AVAILABLE_NOW = bool(PIPELINE.get("available_now", True))
BRONZE_OUTPUT_PARTITIONS = int(PIPELINE.get("bronze_output_partitions", 4))
RECONCILIATION_THRESHOLD_PCT = float(PIPELINE.get("reconciliation_threshold_pct", 0.5))

CONTROL_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.control_ingestion_log"

# Tudo que é arquivo auxiliar da ingestão fica organizado dentro da Landing.
# Não criamos schemas/volumes separados para checkpoint ou schema inference.
#
# Estrutura:
# landing/
#   sample_mflix/
#     users/
#     theaters/
#     comments/
#     movies/
#     _checkpoints/
#       comments/
#       movies/
#     _schemas/
#       comments/
#       movies/
#
# A Bronze continua sendo tabela Delta gerenciada no schema Bronze.
LANDING_BASE_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{VOLUME_NAME}"
CHECKPOINT_BASE_PATH = f"{LANDING_BASE_PATH}/_checkpoints"
SCHEMA_BASE_PATH = f"{LANDING_BASE_PATH}/_schemas"

# Overwrite dinâmico por partição: só a(s) partição(ões) presentes no
# DataFrame são substituídas, o resto da tabela permanece intacto.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# ============================================================
# SETUP DE INFRAESTRUTURA (catalog / schemas / volumes)
# ============================================================
def setup_unity_catalog_objects():
    """
    Cria toda a infraestrutura do Unity Catalog necessária, de forma
    idempotente (IF NOT EXISTS). Assim a pipeline roda em qualquer
    workspace Databricks com Unity Catalog habilitado, mesmo que o
    bronze_job.py seja executado sem o ingestion_job.py ter rodado
    antes nesse workspace.
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{LANDING_SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{LANDING_SCHEMA}.{VOLUME_NAME}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

    # Checkpoints e metadados de schema ficam como diretórios dentro
    # da Landing; não precisam de schema/volume próprio no Unity Catalog.
    dbutils.fs.mkdirs(CHECKPOINT_BASE_PATH)
    dbutils.fs.mkdirs(SCHEMA_BASE_PATH)


# ============================================================
# TABELA DE CONTROLE (R5) — schema alinhado ao ingestion_job.py
# ============================================================
def create_control_table():
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


def get_upstream_ingestion_id(collection: str):
    """
    Recupera o _ingestion_id gerado pelo ingestion_job.py para esta
    coleção, para correlacionar extração e carga na mesma linha da
    tabela de controle.
    Preferência: taskValues (jobs orquestrados como tasks).
    Fallback: última linha 'EXTRACTED' desta coleção na control table
    (para quando o notebook roda avulso).
    """
    try:
        value = dbutils.jobs.taskValues.get(
            taskKey="ingestion_job",
            key=f"ingestion_id__{collection}",
            debugValue=None,
        )
        if value:
            return value
    except Exception:
        pass

    if not spark.catalog.tableExists(CONTROL_TABLE):
        return None
    row = (
        spark.table(CONTROL_TABLE)
        .filter(f"collection = '{collection}' AND status = 'EXTRACTED'")
        .orderBy(F.col("start_time").desc())
        .first()
    )
    return row["_ingestion_id"] if row else None


def reconcile_and_close_control(collection: str, bronze_table: str, ingestion_id: str, start_time: dt.datetime):
    """
    R8 — Reconciliação e qualidade:
      - contagem origem x destino
      - % de nulos em _source_id
      - duplicidade de _source_id no mesmo lote
    Fecha a linha da tabela de controle com o status final.
    """
    end_time = dt.datetime.utcnow()

    if not ingestion_id:
        print(f"[RECONCILIAÇÃO] {collection}: sem _ingestion_id upstream, pulando reconciliação.")
        return

    df_batch = spark.table(bronze_table).filter(F.col("_ingestion_id") == ingestion_id)
    qtd_gravada = df_batch.count()

    null_source_id = df_batch.filter(F.col("_source_id").isNull()).count()
    null_pct = (null_source_id / qtd_gravada * 100) if qtd_gravada else 0.0

    dup_source_id = (
        df_batch.groupBy("_source_id").count().filter("count > 1").count()
    )

    control_row = (
        spark.table(CONTROL_TABLE).filter(f"_ingestion_id = '{ingestion_id}'").first()
    )
    qtd_lida_origem = control_row["qtd_lida_origem"] if control_row else None
    control_start_time = control_row["start_time"] if control_row else start_time

    divergence_pct = None
    if qtd_lida_origem:
        divergence_pct = abs(qtd_lida_origem - qtd_gravada) / qtd_lida_origem * 100

    if qtd_gravada == 0 and (qtd_lida_origem or 0) > 0:
        status = "FAILED"
        mensagem_erro = "Nenhum registro gravado na Bronze apesar de haver dados na origem."
    elif divergence_pct is not None and divergence_pct > RECONCILIATION_THRESHOLD_PCT:
        status = "PARTIAL"
        mensagem_erro = (
            f"Divergência de {divergence_pct:.2f}% entre origem e destino "
            f"(limiar configurado: {RECONCILIATION_THRESHOLD_PCT}%)."
        )
    elif dup_source_id > 0:
        status = "PARTIAL"
        mensagem_erro = f"{dup_source_id} valores de _source_id duplicados no lote."
    elif null_pct > 0:
        status = "PARTIAL"
        mensagem_erro = f"{null_pct:.2f}% dos registros com _source_id nulo."
    else:
        status = "SUCCESS"
        mensagem_erro = None

    duracao_seg = (end_time - control_start_time).total_seconds()

    updates_df = spark.createDataFrame(
        [(ingestion_id, qtd_gravada, end_time, duracao_seg, status, mensagem_erro)],
        schema=(
            "_ingestion_id STRING, qtd_gravada_destino BIGINT, end_time TIMESTAMP, "
            "duracao_seg DOUBLE, status STRING, mensagem_erro STRING"
        ),
    )

    (
        DeltaTable.forName(spark, CONTROL_TABLE)
        .alias("t")
        .merge(updates_df.alias("s"), "t._ingestion_id = s._ingestion_id")
        .whenMatchedUpdate(
            set={
                "qtd_gravada_destino": "s.qtd_gravada_destino",
                "end_time": "s.end_time",
                "duracao_seg": "s.duracao_seg",
                "status": "s.status",
                "mensagem_erro": "s.mensagem_erro",
            }
        )
        .execute()
    )

    print(
        f"[RECONCILIAÇÃO] {collection}: origem={qtd_lida_origem} destino={qtd_gravada} "
        f"nulos_source_id={null_pct:.2f}% duplicados={dup_source_id} status={status}"
    )


# ============================================================
# METADADOS DE LINHAGEM (R4)
# ============================================================
def add_bronze_metadata(df, collection_cfg, ingestion_id, load_type):
    return (
        df
        # UUID da execução (run id) -- fixo para todo o lote, não por linha
        .withColumn("_ingestion_id", F.lit(ingestion_id))
        # Timestamp UTC da gravação
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        # Sistema de origem
        .withColumn("_source_path", F.lit("mongodb_atlas"))
        # Arquivo físico na Landing que originou o registro (lineage extra)
        .withColumn("_landing_file_path", F.input_file_name())
        # full ou incremental
        .withColumn("_load_type", F.lit(load_type))
        # Data técnica -- também é a coluna de partição física
        .withColumn("_ingestion_date", F.current_date())
        # Collection MongoDB de origem
        .withColumn("_source_collection", F.lit(collection_cfg["collection"]))
        # Chave natural do MongoDB, usada na reconciliação (R8)
        .withColumn(
            "_source_id",
            F.col("_id").cast("string") if "_id" in df.columns else F.lit(None).cast("string"),
        )
    )


# ============================================================
# CARGA FULL — batch, overwrite dinâmico da partição do dia
# ============================================================
def load_to_bronze_full(collection_cfg, ingestion_id):
    collection = collection_cfg["collection"]
    destination = collection_cfg["destino"]
    landing_path = f"{LANDING_BASE_PATH.rstrip('/')}/{collection}"
    bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{destination}"

    print("=" * 80)
    print(f"BRONZE (FULL): {collection}")
    print(f"Landing: {landing_path}")
    print(f"Tabela: {bronze_table}")
    print("Estrutura física: partitionBy(_ingestion_date) dentro do storage gerenciado pelo UC")
    print("=" * 80)

    # R7 -- schema drift: mode PERMISSIVE preserva registros que não
    # batem com o schema inferido em vez de descartá-los.
    df_raw = (
        spark.read.format("json")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_rescued_data")
        .load(landing_path)
    )

    df_bronze = add_bronze_metadata(
        df_raw, collection_cfg, ingestion_id, collection_cfg["modo_carga"]
    )

    (
        df_bronze
        # R2.3 -- controle de partições/anti small-files na escrita
        .repartition(BRONZE_OUTPUT_PARTITIONS)
        .write.format("delta")
        .mode("overwrite")  # dinâmico: só sobrescreve a partição do dia
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_date")
        .saveAsTable(bronze_table)
    )

    print(f"Bronze (full) concluída: {bronze_table}")


# ============================================================
# CARGA INCREMENTAL — Auto Loader streaming, append + checkpoint
# ============================================================
def load_to_bronze_incremental(collection_cfg, ingestion_id):
    collection = collection_cfg["collection"]
    destination = collection_cfg["destino"]

    landing_path = f"{LANDING_BASE_PATH.rstrip('/')}/{collection}"
    checkpoint_path = f"{CHECKPOINT_BASE_PATH.rstrip('/')}/{collection}"
    schema_path = f"{SCHEMA_BASE_PATH.rstrip('/')}/{collection}"
    bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{destination}"

    print("=" * 80)
    print(f"BRONZE (INCREMENTAL): {collection}")
    print(f"Landing: {landing_path}")
    print(f"Schema inference: {schema_path}")
    print(f"Checkpoint: {checkpoint_path}")
    print(f"Tabela: {bronze_table}")
    print("Estrutura física: partitionBy(_ingestion_date) dentro do storage gerenciado pelo UC")
    print("=" * 80)

    df_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", str(INFER_COLUMN_TYPES).lower())
        # R7 -- schema evolution + coluna de quarentena para campos
        # inesperados (nunca descartados silenciosamente)
        .option("cloudFiles.schemaEvolutionMode", SCHEMA_EVOLUTION_MODE)
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.includeExistingFiles", "true")
        .load(landing_path)
    )

    df_bronze = add_bronze_metadata(
        df_stream, collection_cfg, ingestion_id, collection_cfg["modo_carga"]
    )

    query = (
        df_bronze.repartition(BRONZE_OUTPUT_PARTITIONS)
        .writeStream.format("delta")
        .outputMode("append")  # idempotente: checkpoint evita reprocessar arquivo já lido
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_date")
        .trigger(availableNow=AVAILABLE_NOW)
        .toTable(bronze_table)
    )

    query.awaitTermination()
    print(f"Bronze (incremental) concluída: {bronze_table}")


# ============================================================
# EXECUÇÃO
# ============================================================
setup_unity_catalog_objects()
create_control_table()

for collection_cfg in COLLECTIONS:
    if not collection_cfg.get("enabled", True):
        continue

    collection = collection_cfg["collection"]
    destination = collection_cfg["destino"]
    bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{destination}"
    start_time = dt.datetime.utcnow()

    ingestion_id = get_upstream_ingestion_id(collection)

    if collection_cfg["modo_carga"] == "full":
        load_to_bronze_full(collection_cfg, ingestion_id)
    else:
        load_to_bronze_incremental(collection_cfg, ingestion_id)

    reconcile_and_close_control(collection, bronze_table, ingestion_id, start_time)
