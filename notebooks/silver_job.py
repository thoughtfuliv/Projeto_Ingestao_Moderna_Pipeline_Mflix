# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
"""Materializa a camada Silver do Mflix a partir das tabelas Delta Bronze.

O job valida e coloca registros invalidos em quarentena, padroniza tipos,
remove duplicidades e usa MERGE para manter a carga idempotente.
"""

import datetime
import json
import re
import uuid
from pathlib import Path

import yaml
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

def resolve_project_file(relative_path: str) -> Path:
    """Localiza um arquivo do projeto a partir da raiz ou de notebooks/."""
    candidates = (Path.cwd() / relative_path, Path.cwd().parent / relative_path)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    checked = ", ".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(
        f"Arquivo do projeto nao encontrado: {relative_path}. Caminhos testados: {checked}"
    )


PIPELINE_CONFIG_PATH = resolve_project_file("config/pipeline_config.yaml")
COLLECTIONS_CONFIG_PATH = resolve_project_file("config/collections.json")

with open(PIPELINE_CONFIG_PATH, "r", encoding="utf-8") as file:
    PIPELINE = yaml.safe_load(file)["pipeline"]

with open(COLLECTIONS_CONFIG_PATH, "r", encoding="utf-8") as file:
    COLLECTIONS_CONFIG = json.load(file)

CATALOG = PIPELINE["catalog"]
BRONZE_SCHEMA = PIPELINE["bronze_schema"]
SILVER_SCHEMA = PIPELINE["silver_schema"]
QUALITY_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.control_quality_log"
NULL_KEY_THRESHOLD_PCT = float(PIPELINE.get("silver_null_key_threshold_pct", 0.0))


def validate_identifier(value: str) -> str:
    """Valida nomes interpolados em comandos SQL."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Identificador invalido: {value!r}")
    return value


for identifier in (CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA):
    validate_identifier(identifier)


# COMMAND ----------

QUALITY_LOG_SCHEMA = StructType([
    StructField("execution_id", StringType(), False),
    StructField("collection", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("source_count", LongType(), False),
    StructField("valid_count", LongType(), False),
    StructField("quarantine_count", LongType(), False),
    StructField("null_key_count", LongType(), False),
    StructField("duplicate_key_count", LongType(), False),
    StructField("null_key_pct", DoubleType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), False),
    StructField("duration_seconds", DoubleType(), False),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True),
])


def create_control_objects() -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
    spark.createDataFrame([], QUALITY_LOG_SCHEMA).write.format("delta").mode(
        "ignore"
    ).saveAsTable(QUALITY_TABLE)


def column_or_null(df: DataFrame, name: str):
    return F.col(name) if name in df.columns else F.lit(None)


def flatten_json_fields(df: DataFrame) -> DataFrame:
    """Converte objetos JSON (StructType) em colunas SQL escalares.

    O schema explicito da Bronze ja faz o parse do JSON. Aqui removemos os
    structs restantes para que consultas SQL nao precisem acessar campos
    com a notacao objeto.campo. Arrays e maps permanecem tipos SQL nativos.
    """
    projections = []
    output_names = set()
    column_aliases = {
        ("tomatoes", "critic", "numReviews"): "tomatoes_critic_reviews",
        ("tomatoes", "viewer", "numReviews"): "tomatoes_viewer_reviews",
    }

    def append_field(path: list[str], data_type) -> None:
        if isinstance(data_type, StructType):
            for child in data_type.fields:
                append_field([*path, child.name], child.dataType)
            return

        output_name = column_aliases.get(tuple(path), "_".join(path))
        if output_name in output_names:
            dotted_path = ".".join(path)
            raise ValueError(
                f"Colisao ao converter campo JSON para SQL: {dotted_path!r} "
                f"gera a coluna duplicada {output_name!r}"
            )
        output_names.add(output_name)
        source = F.col(".".join(f"`{part}`" for part in path))
        projections.append(source.alias(output_name))

    for field in df.schema.fields:
        append_field([field.name], field.dataType)

    return df.select(*projections)


def add_validation_columns(df: DataFrame) -> DataFrame:
    """Classifica registros sem descarta-los silenciosamente."""
    source_id = F.trim(column_or_null(df, "_source_id").cast("string"))
    corrupt = column_or_null(df, "_corrupt_record").isNotNull()
    rescued_value = column_or_null(df, "_rescued_data").cast("string")
    rescued = (
        rescued_value.isNotNull()
        & (F.trim(rescued_value) != "")
        & (F.trim(rescued_value) != "{}")
    )
    reason = (
        F.when(source_id.isNull() | (source_id == ""), F.lit("NULL_SOURCE_ID"))
        .when(corrupt, F.lit("CORRUPT_RECORD"))
        .when(rescued, F.lit("RESCUED_DATA"))
    )
    return (
        df.withColumn("_source_id", source_id)
        .withColumn("_quarantine_reason", reason)
        .withColumn("_is_valid", F.col("_quarantine_reason").isNull())
    )


def normalize_business_types(df: DataFrame, collection: str) -> DataFrame:
    """Aplica padronizacoes deterministicas adequadas a Silver."""
    timestamp_columns = {
        "movies": ("released", "lastupdated", "tomatoes_lastUpdated"),
        "comments": ("date",),
    }
    result = df
    for column_name in timestamp_columns.get(collection, ()):
        if column_name in result.columns:
            result = result.withColumn(
                column_name, F.expr(f"try_cast(`{column_name}` as timestamp)")
            )
    if "email" in result.columns:
        result = result.withColumn("email", F.lower(F.trim(F.col("email"))))

    technical_errors = [
        name
        for name in ("_corrupt_record", "_rescued_data", "_is_valid", "_quarantine_reason")
        if name in result.columns
    ]
    return result.drop(*technical_errors).withColumn(
        "_silver_timestamp", F.current_timestamp()
    )


def latest_by_source_id(df: DataFrame) -> DataFrame:
    """Seleciona a versao mais recente de cada documento MongoDB."""
    ordering = []
    if "_ingestion_timestamp" in df.columns:
        ordering.append(F.col("_ingestion_timestamp").desc_nulls_last())
    if "_ingestion_id" in df.columns:
        ordering.append(F.col("_ingestion_id").desc_nulls_last())
    if not ordering:
        ordering.append(F.col("_source_id"))
    window = Window.partitionBy("_source_id").orderBy(*ordering)
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def add_record_hash(df: DataFrame) -> DataFrame:
    """Cria assinatura estavel do conteudo de negocio."""
    business_columns = sorted(
        column
        for column in df.columns
        if not column.startswith("_ingestion")
        and column
        not in {"_silver_timestamp", "_quarantine_timestamp", "_record_hash"}
    )
    payload = F.to_json(F.struct(*[F.col(name) for name in business_columns]))
    return df.withColumn("_record_hash", F.sha2(payload, 256))


def merge_silver(df: DataFrame, target_table: str) -> None:
    """Insere documentos novos e atualiza apenas documentos alterados."""
    if not spark.catalog.tableExists(target_table):
        df.write.format("delta").option("mergeSchema", "true").mode(
            "overwrite"
        ).saveAsTable(target_table)
        return

    target_schema = spark.table(target_table).schema
    target_columns = {field.name for field in target_schema.fields}

    if "_record_hash" not in target_columns:
        print(f"SILVER {target_table}: adicionando coluna técnica _record_hash")
        spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS (_record_hash STRING)")
        target_schema = spark.table(target_table).schema

    # Tabelas criadas anteriormente por SQL podem conter colunas que nao
    # existem mais no contrato atual. Inclui essas colunas como NULL na fonte
    # para que UPDATE ALL seja resolvido sem apagar/recriar a tabela.
    aligned_df = df
    for field in target_schema.fields:
        if field.name not in aligned_df.columns:
            aligned_df = aligned_df.withColumn(
                field.name, F.lit(None).cast(field.dataType)
            )

    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(aligned_df.alias("source"), "target._source_id = source._source_id")
        .withSchemaEvolution()
        .whenMatchedUpdateAll(
            condition="target._record_hash <> source._record_hash OR target._record_hash IS NULL"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_quarantine(df: DataFrame, quarantine_table: str) -> None:
    """Persiste invalidos uma unica vez por conteudo e motivo."""
    if df.limit(1).count() == 0:
        return
    quarantined = add_record_hash(
        df.withColumn("_quarantine_timestamp", F.current_timestamp())
    )
    if not spark.catalog.tableExists(quarantine_table):
        quarantined.write.format("delta").option("mergeSchema", "true").mode(
            "overwrite"
        ).saveAsTable(quarantine_table)
        return
    (
        DeltaTable.forName(spark, quarantine_table)
        .alias("target")
        .merge(
            quarantined.alias("source"),
            "target._record_hash = source._record_hash "
            "AND target._quarantine_reason = source._quarantine_reason",
        )
        .withSchemaEvolution()
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_quality_log(values: dict) -> None:
    spark.createDataFrame([values], QUALITY_LOG_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(QUALITY_TABLE)


# COMMAND ----------

def process_collection(collection_cfg: dict) -> None:
    collection = validate_identifier(collection_cfg["collection"])
    source_table = f"{CATALOG}.{BRONZE_SCHEMA}.{collection}"
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.{collection}"
    quarantine_table = f"{CATALOG}.{SILVER_SCHEMA}.quarantine_{collection}"
    execution_id = str(uuid.uuid4())
    start_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    metrics = {
        "source_count": 0,
        "valid_count": 0,
        "quarantine_count": 0,
        "null_key_count": 0,
        "duplicate_key_count": 0,
        "null_key_pct": 0.0,
    }
    status, error_message = "SUCCESS", None

    try:
        if not spark.catalog.tableExists(source_table):
            raise RuntimeError(f"Tabela Bronze nao encontrada: {source_table}")

        # Databricks Serverless nao oferece suporte a CACHE/PERSIST TABLE.
        # Mantemos o DataFrame lazy para evitar uma operacao nao suportada.
        validated = add_validation_columns(spark.table(source_table))
        metrics["source_count"] = validated.count()
        metrics["null_key_count"] = validated.filter(
            F.col("_quarantine_reason") == "NULL_SOURCE_ID"
        ).count()
        metrics["quarantine_count"] = validated.filter(~F.col("_is_valid")).count()
        metrics["null_key_pct"] = (
            metrics["null_key_count"] * 100.0 / metrics["source_count"]
            if metrics["source_count"]
            else 0.0
        )
        valid = validated.filter(F.col("_is_valid"))
        metrics["duplicate_key_count"] = (
            valid.groupBy("_source_id").count().filter(F.col("count") > 1).count()
        )

        merge_quarantine(validated.filter(~F.col("_is_valid")), quarantine_table)
        latest = latest_by_source_id(valid)
        silver = add_record_hash(
            normalize_business_types(flatten_json_fields(latest), collection)
        )
        metrics["valid_count"] = silver.count()
        merge_silver(silver, target_table)

        if (
            metrics["null_key_pct"] > NULL_KEY_THRESHOLD_PCT
            or metrics["quarantine_count"] > 0
        ):
            status = "PARTIAL"
    except Exception as exc:
        status, error_message = "FAILED", str(exc)[:1000]

    end_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    write_quality_log({
        "execution_id": execution_id,
        "collection": collection,
        "source_table": source_table,
        "target_table": target_table,
        **metrics,
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": (end_time - start_time).total_seconds(),
        "status": status,
        "error_message": error_message,
    })
    print(
        f"SILVER {collection}: status={status}, origem={metrics['source_count']}, "
        f"validos={metrics['valid_count']}, quarentena={metrics['quarantine_count']}, "
        f"chaves_duplicadas={metrics['duplicate_key_count']}"
    )
    if status == "FAILED":
        raise RuntimeError(error_message)


def main() -> None:
    create_control_objects()
    for collection_cfg in COLLECTIONS_CONFIG:
        if collection_cfg.get("enabled", True):
            process_collection(collection_cfg)


if __name__ == "__main__":
    main()