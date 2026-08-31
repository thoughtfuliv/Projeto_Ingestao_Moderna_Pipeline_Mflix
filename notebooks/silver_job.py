# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
"""Materializa a camada Silver do Mflix a partir das tabelas Delta Bronze.

O job padroniza tipos, remove duplicidades e usa MERGE para manter a carga
idempotente. Todos os registros da Bronze sao processados e mergeados na
Silver, independentemente de _source_id nulo/vazio, _corrupt_record ou
_rescued_data — nao ha mais classificacao de "quarentena": as metricas de
chave nula/duplicada em control_quality_log sao apenas informativas.
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
    ArrayType,
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

    # _corrupt_record e _rescued_data sao colunas tecnicas da Bronze (nao
    # sao mais usadas para filtrar registros) — removidas aqui so por
    # higiene de schema antes de escrever na Silver.
    technical_errors = [
        name
        for name in ("_corrupt_record", "_rescued_data")
        if name in result.columns
    ]
    return result.drop(*technical_errors).withColumn(
        "_silver_timestamp", F.current_timestamp()
    )


def latest_by_source_id(df: DataFrame) -> DataFrame:
    """Seleciona a versao mais recente de cada documento MongoDB.

    Criterio de "mais recente", em ordem de prioridade:
    1. _ingestion_timestamp — quando a Bronze gravou o registro.
    2. _ingestion_id — identifica a execucao de ingestao (mas e o MESMO
       valor para todas as linhas de uma execucao, entao nao desempata
       documentos diferentes ingeridos juntos).
    3. hash deterministico do conteudo do registro — desempate estavel
       quando os dois criterios acima empatam (ex.: dois documentos com o
       mesmo _source_id chegando no mesmo microbatch/execucao). Sem isso o
       Spark escolhe um vencedor arbitrario, que pode mudar entre
       execucoes e quebrar a idempotencia da carga.
    """
    ordering = []
    if "_ingestion_timestamp" in df.columns:
        ordering.append(F.col("_ingestion_timestamp").desc_nulls_last())
    if "_ingestion_id" in df.columns:
        ordering.append(F.col("_ingestion_id").desc_nulls_last())

    tiebreak_columns = sorted(column for column in df.columns if column != "_source_id")
    df = df.withColumn(
        "_dedup_tiebreak", F.hash(*[F.col(column) for column in tiebreak_columns])
    )
    ordering.append(F.col("_dedup_tiebreak").desc())

    window = Window.partitionBy("_source_id").orderBy(*ordering)
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number", "_dedup_tiebreak")
    )


def add_record_hash(df: DataFrame) -> DataFrame:
    """Cria assinatura estavel do conteudo de negocio."""
    business_columns = sorted(
        column
        for column in df.columns
        if not column.startswith("_ingestion")
        and column
        not in {"_silver_timestamp", "_record_hash"}
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


# Campos array de alta dimensionalidade (vetores de embedding) que nao devem
# ser explodidos: cada documento geraria milhares de linhas sem valor
# analitico. Ajuste os nomes aqui se o campo do seu embedded_movies tiver
# outro nome.
EXCLUDED_ARRAY_COLUMNS = {"embedding", "plot_embedding"}


def explode_array_columns(df: DataFrame) -> tuple[DataFrame, bool]:
    """Explode toda coluna do tipo array do DataFrame, duplicando a linha
    para cada item (explode_outer preserva o registro quando o array e
    nulo/vazio, em vez de descarta-lo como explode faria). Quando ha mais
    de uma coluna array, os explodes sao encadeados: a linha e duplicada
    para cada combinacao dos itens.

    Retorna (dataframe_resultante, houve_explode). Quando houve_explode e
    True, _source_id deixa de ser unico por linha — quem grava a tabela
    precisa usar overwrite completo, nao MERGE (ver write_silver).
    """
    result = df
    exploded_any = False
    for field in df.schema.fields:
        if not isinstance(field.dataType, ArrayType):
            continue
        if field.name in EXCLUDED_ARRAY_COLUMNS:
            continue
        result = result.withColumn(field.name, F.explode_outer(F.col(field.name)))
        exploded_any = True
    return result, exploded_any


def write_silver(df: DataFrame, target_table: str, allow_merge: bool) -> None:
    """Grava a Silver.

    Usa MERGE idempotente por _source_id quando possivel (allow_merge=True).
    Quando a linha foi duplicada por explode de array, _source_id deixa de
    ser chave unica por linha e o MERGE quebraria (source nao pode ter mais
    de uma linha por chave) — nesses casos faz overwrite completo da tabela,
    que continua idempotente (mesma entrada sempre gera a mesma saida), so
    que recalculado do zero em vez de incremental.
    """
    if allow_merge:
        merge_silver(df, target_table)
        return

    df.write.format("delta").option("mergeSchema", "true").mode(
        "overwrite"
    ).saveAsTable(target_table)


def write_quality_log(values: dict) -> None:
    (
        spark.createDataFrame([values], QUALITY_LOG_SCHEMA)
        .write.format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .saveAsTable(QUALITY_TABLE)
    )


# COMMAND ----------

def process_collection(collection_cfg: dict) -> None:
    collection = validate_identifier(collection_cfg["collection"])
    source_table = f"{CATALOG}.{BRONZE_SCHEMA}.{collection}"
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.{collection}"
    execution_id = str(uuid.uuid4())
    start_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    metrics = {
        "source_count": 0,
        "valid_count": 0,
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
        source = spark.table(source_table)

        # BUGFIX: a Bronze nunca teve uma coluna "_source_id" — a chave do
        # documento MongoDB chega como "_id" (nome original do JSON exportado
        # pelo ingestion_job.py). Sem esse rename, column_or_null(df,
        # "_source_id") sempre caia no else e retornava null para toda
        # linha, fazendo o filtro de chave unica descartar tudo.
        if "_source_id" not in source.columns and "_id" in source.columns:
            source = source.withColumnRenamed("_id", "_source_id")

        metrics["source_count"] = source.count()
        source_id = F.trim(column_or_null(source, "_source_id").cast("string"))
        metrics["null_key_count"] = source.filter(
            source_id.isNull() | (source_id == "")
        ).count()
        metrics["null_key_pct"] = (
            metrics["null_key_count"] * 100.0 / metrics["source_count"]
            if metrics["source_count"]
            else 0.0
        )

        # Sem classificacao de quarentena para _corrupt_record ou
        # _rescued_data: esses registros sobem normalmente. A unica
        # excecao e o controle de chave: so sobe dado com _source_id
        # unico. Registros com _source_id nulo/vazio nao tem como ser
        # deduplicados de forma idempotente entre execucoes (o MERGE
        # nunca compara NULL = NULL como igual, entao um registro sem
        # chave seria inserido de novo a cada execucao) — por isso sao
        # os unicos excluidos aqui. null_key_count/duplicate_key_count
        # continuam registrados no control_quality_log.
        all_records = source.withColumn("_source_id", source_id)
        metrics["duplicate_key_count"] = (
            all_records.groupBy("_source_id").count().filter(F.col("count") > 1).count()
        )

        with_unique_key = all_records.filter(
            source_id.isNotNull() & (source_id != "")
        )

        latest = latest_by_source_id(with_unique_key)
        silver = add_record_hash(
            normalize_business_types(flatten_json_fields(latest), collection)
        )
        silver, exploded = explode_array_columns(silver)
        metrics["valid_count"] = silver.count()
        write_silver(silver, target_table, allow_merge=not exploded)
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
        f"processados={metrics['valid_count']}, "
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