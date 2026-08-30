
# Databricks notebook source

# ============================================================
# BRONZE JOB
# Landing -> Bronze
# ============================================================

import datetime as dt
import json

import yaml

from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIGURAÇÃO
# ============================================================

CONFIG_DIR = (
    "/Workspace/Users/lcspinheiro17@gmail.com/Projeto_Ingestao_Moderna_Pipeline_Mflix/config"
)


# ============================================================
# PIPELINE CONFIG
# ============================================================

with open(
    f"{CONFIG_DIR}/pipeline_config.yaml",
    "r",
    encoding="utf-8"
) as f:

    CONFIG = yaml.safe_load(f)


# ============================================================
# COLLECTIONS CONFIG
# ============================================================

with open(
    f"{CONFIG_DIR}/collections.json",
    "r",
    encoding="utf-8"
) as f:

    COLLECTIONS = json.load(f)


# ============================================================
# CONFIGURAÇÕES
# ============================================================

PIPELINE = CONFIG["pipeline"]


CATALOG = PIPELINE["catalog"]

LANDING_SCHEMA = PIPELINE["landing_schema"]

BRONZE_SCHEMA = PIPELINE["bronze_schema"]

VOLUME_NAME = PIPELINE["volume_name"]


INFER_COLUMN_TYPES = bool(
    PIPELINE.get(
        "infer_column_types",
        True
    )
)


SCHEMA_EVOLUTION_MODE = PIPELINE.get(
    "schema_evolution_mode",
    "rescue"
)


BRONZE_OUTPUT_PARTITIONS = int(
    PIPELINE.get(
        "bronze_output_partitions",
        4
    )
)


# ============================================================
# PATH DA LANDING
# ============================================================
#
# O pipeline_config.yaml DEVE conter:
#
# catalog: meu_catalog
# landing_schema: landing
# volume_name: mflix
#
# Resultado:
#
# /Volumes/meu_catalog/landing/mflix
#
# NÃO colocar "landing" dentro de VOLUME_NAME.
# ============================================================

LANDING_BASE_PATH = (
    f"/Volumes/"
    f"{CATALOG}/"
    f"{LANDING_SCHEMA}/"
    f"{VOLUME_NAME}"
)


# ============================================================
# CHECKPOINT E SCHEMA
# ============================================================
#
# Tudo fica dentro do próprio Volume.
# ============================================================

CHECKPOINT_BASE_PATH = (
    f"{LANDING_BASE_PATH}/_checkpoints"
)


SCHEMA_BASE_PATH = (
    f"{LANDING_BASE_PATH}/_schemas"
)


# ============================================================
# TABELA DE CONTROLE
# ============================================================

CONTROL_TABLE = (
    f"{CATALOG}."
    f"{BRONZE_SCHEMA}."
    f"control_ingestion_log"
)


# ============================================================
# VALIDAR CAMINHO
# ============================================================

print("=" * 80)

print("CONFIGURAÇÃO DA BRONZE")

print(f"CATALOG:          {CATALOG}")
print(f"LANDING_SCHEMA:   {LANDING_SCHEMA}")
print(f"BRONZE_SCHEMA:    {BRONZE_SCHEMA}")
print(f"VOLUME_NAME:      {VOLUME_NAME}")

print(
    f"LANDING_BASE_PATH: "
    f"{LANDING_BASE_PATH}"
)

print(
    f"CHECKPOINT_BASE_PATH: "
    f"{CHECKPOINT_BASE_PATH}"
)

print(
    f"SCHEMA_BASE_PATH: "
    f"{SCHEMA_BASE_PATH}"
)

print("=" * 80)


# ============================================================
# SETUP
# ============================================================

def setup_unity_catalog_objects():

    """
    Cria somente os schemas/tabelas necessários.

    Checkpoint e schema inference NÃO são schemas
    do Unity Catalog. São apenas diretórios dentro
    do Volume.
    """

    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS
        {CATALOG}.{LANDING_SCHEMA}
        """
    )


    spark.sql(
        f"""
        CREATE VOLUME IF NOT EXISTS
        {CATALOG}.{LANDING_SCHEMA}.{VOLUME_NAME}
        """
    )


    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS
        {CATALOG}.{BRONZE_SCHEMA}
        """
    )


# ============================================================
# TABELA DE CONTROLE
# ============================================================

def create_control_table():

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
        {CONTROL_TABLE}
        (
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
# INGESTION ID
# ============================================================

def get_ingestion_id(collection):

    """
    Tenta recuperar o ingestion_id do ingestion_job.

    Caso esteja executando o notebook isoladamente,
    gera um ID local.
    """

    try:

        value = dbutils.jobs.taskValues.get(
            taskKey="ingestion",
            key=f"ingestion_id__{collection}",
            default="",
            debugValue=""
        )

        if value:

            return value

    except Exception:

        pass


    return (
        f"bronze_"
        f"{collection}_"
        f"{dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    )


# ============================================================
# METADADOS DA BRONZE
# ============================================================

def add_bronze_metadata(
    df,
    collection_cfg,
    ingestion_id
):

    collection = collection_cfg["collection"]

    load_type = collection_cfg["modo_carga"]

    database = collection_cfg["database"]


    df = (
        df

        # Data da ingestão
        .withColumn(
            "_ingestion_date",
            F.current_date()
        )

        # Timestamp
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp()
        )

        # ID da execução
        .withColumn(
            "_ingestion_id",
            F.lit(ingestion_id)
        )

        # Sistema de origem
        .withColumn(
            "_source_system",
            F.lit("mongodb_atlas")
        )

        # Database de origem
        .withColumn(
            "_source_database",
            F.lit(database)
        )

        # Collection de origem
        .withColumn(
            "_source_collection",
            F.lit(collection)
        )

        # Tipo de carga
        .withColumn(
            "_load_type",
            F.lit(load_type)
        )
    )


    return df


# ============================================================
# DEDUPLICAÇÃO
# ============================================================

def deduplicate_by_id(df):

    """
    Remove duplicidades dentro do próprio batch.

    A chave utilizada é _id.
    """

    if "_id" not in df.columns:

        raise ValueError(
            "A coleção não possui _id. "
            "Não é possível garantir "
            "idempotência por chave."
        )


    window_spec = (

        Window

        .partitionBy("_id")

        .orderBy(
            F.col(
                "_ingestion_timestamp"
            ).desc()
        )
    )


    return (

        df

        .withColumn(
            "_row_number",
            F.row_number().over(
                window_spec
            )
        )

        .filter(
            F.col("_row_number") == 1
        )

        .drop("_row_number")
    )


# ============================================================
# MERGE IDEMPOTENTE
# ============================================================

def merge_to_bronze(
    microbatch_df,
    bronze_table
):

    """
    MERGE por _id.

    Se _id já existir:
        UPDATE

    Se _id não existir:
        INSERT
    """

    microbatch_df = deduplicate_by_id(
        microbatch_df
    )


    # --------------------------------------------------------
    # Primeira execução
    # --------------------------------------------------------

    if not spark.catalog.tableExists(
        bronze_table
    ):

        (

            microbatch_df

            .repartition(
                BRONZE_OUTPUT_PARTITIONS
            )

            .write

            .format("delta")

            .mode("append")

            .partitionBy(
                "_ingestion_date"
            )

            .saveAsTable(
                bronze_table
            )
        )

        return


    # --------------------------------------------------------
    # Tabela Delta existente
    # --------------------------------------------------------

    target = DeltaTable.forName(
        spark,
        bronze_table
    )


    columns = microbatch_df.columns


    # --------------------------------------------------------
    # UPDATE
    # --------------------------------------------------------

    update_values = {

        column:
            f"source.`{column}`"

        for column in columns

        if column != "_id"
    }


    # --------------------------------------------------------
    # INSERT
    # --------------------------------------------------------

    insert_values = {

        column:
            f"source.`{column}`"

        for column in columns
    }


    # --------------------------------------------------------
    # MERGE
    # --------------------------------------------------------

    (

        target.alias("target")

        .merge(

            microbatch_df.alias("source"),

            "target.`_id` = source.`_id`"
        )

        .whenMatchedUpdate(
            set=update_values
        )

        .whenNotMatchedInsert(
            values=insert_values
        )

        .execute()
    )


# ============================================================
# INCREMENTAL
# ============================================================

def load_incremental(
    collection_cfg,
    ingestion_id
):

    collection = collection_cfg["collection"]

    destination = collection_cfg["destino"]


    # --------------------------------------------------------
    # CAMINHOS
    # --------------------------------------------------------

    landing_path = (
        f"{LANDING_BASE_PATH}/"
        f"{collection}"
    )


    checkpoint_path = (
        f"{CHECKPOINT_BASE_PATH}/"
        f"{collection}"
    )


    schema_path = (
        f"{SCHEMA_BASE_PATH}/"
        f"{collection}"
    )


    bronze_table = (
        f"{CATALOG}."
        f"{BRONZE_SCHEMA}."
        f"{destination}"
    )


    print("=" * 80)

    print(
        f"INCREMENTAL: {collection}"
    )

    print(
        f"Landing: {landing_path}"
    )

    print(
        f"Checkpoint: {checkpoint_path}"
    )

    print(
        f"Schema: {schema_path}"
    )

    print(
        f"Bronze: {bronze_table}"
    )

    print("=" * 80)


    # --------------------------------------------------------
    # AUTO LOADER
    # --------------------------------------------------------

    df_stream = (

        spark.readStream

        .format("cloudFiles")

        .option(
            "cloudFiles.format",
            "json"
        )

        .option(
            "cloudFiles.schemaLocation",
            schema_path
        )

        .option(
            "cloudFiles.inferColumnTypes",
            str(
                INFER_COLUMN_TYPES
            ).lower()
        )

        .option(
            "cloudFiles.schemaEvolutionMode",
            SCHEMA_EVOLUTION_MODE
        )

        # ----------------------------------------------------
        # Schema drift
        #
        # Dados que não encaixarem no schema
        # ficam em _rescued_data.
        #
        # NÃO existe tabela de quarentena.
        # ----------------------------------------------------

        .option(
            "cloudFiles.rescuedDataColumn",
            "_rescued_data"
        )

        .option(
            "cloudFiles.includeExistingFiles",
            "true"
        )

        .load(
            landing_path
        )
    )


    # --------------------------------------------------------
    # METADADOS
    # --------------------------------------------------------

    df_bronze = add_bronze_metadata(
        df_stream,
        collection_cfg,
        ingestion_id
    )


    # --------------------------------------------------------
    # PROCESSA MICRO-BATCH
    # --------------------------------------------------------

    def process_microbatch(
        microbatch_df,
        batch_id
    ):

        print(
            f"Processando batch "
            f"{batch_id}"
        )


        merge_to_bronze(
            microbatch_df,
            bronze_table
        )


    # --------------------------------------------------------
    # STREAMING
    # --------------------------------------------------------

    query = (

        df_bronze

        .writeStream

        .foreachBatch(
            process_microbatch
        )

        .option(
            "checkpointLocation",
            checkpoint_path
        )

        .trigger(
            availableNow=True
        )

        .start()
    )


    query.awaitTermination()


    print(
        f"Incremental finalizada: "
        f"{bronze_table}"
    )


# ============================================================
# FULL
# ============================================================

def load_full(
    collection_cfg,
    ingestion_id
):

    collection = collection_cfg["collection"]

    destination = collection_cfg["destino"]


    # --------------------------------------------------------
    # LANDING
    # --------------------------------------------------------

    landing_path = (
        f"{LANDING_BASE_PATH}/"
        f"{collection}"
    )


    # --------------------------------------------------------
    # BRONZE
    # --------------------------------------------------------

    bronze_table = (
        f"{CATALOG}."
        f"{BRONZE_SCHEMA}."
        f"{destination}"
    )


    print("=" * 80)

    print(
        f"FULL: {collection}"
    )

    print(
        f"Landing: {landing_path}"
    )

    print(
        f"Bronze: {bronze_table}"
    )

    print("=" * 80)


    # --------------------------------------------------------
    # LEITURA DO SNAPSHOT
    # --------------------------------------------------------

    df = (

        spark.read

        .format("json")

        .option(
            "recursiveFileLookup",
            "true"
        )

        .option(
            "rescuedDataColumn",
            "_rescued_data"
        )

        .load(
            landing_path
        )
    )


    # --------------------------------------------------------
    # METADADOS
    # --------------------------------------------------------

    df = add_bronze_metadata(
        df,
        collection_cfg,
        ingestion_id
    )


    # --------------------------------------------------------
    # DEDUPLICAÇÃO
    # --------------------------------------------------------

    if "_id" in df.columns:

        df = deduplicate_by_id(
            df
        )


    # --------------------------------------------------------
    # FULL SNAPSHOT
    # --------------------------------------------------------
    #
    # Substitui somente a tabela da coleção.
    #
    # Não usa:
    #
    # spark.sql.sources.partitionOverwriteMode
    #
    # pois essa configuração não está
    # disponível no ambiente.
    # --------------------------------------------------------

    (

        df

        .repartition(
            BRONZE_OUTPUT_PARTITIONS
        )

        .write

        .format("delta")

        .mode("overwrite")

        .option(
            "overwriteSchema",
            "true"
        )

        .partitionBy(
            "_ingestion_date"
        )

        .saveAsTable(
            bronze_table
        )
    )


    print(
        f"Full finalizada: "
        f"{bronze_table}"
    )


# ============================================================
# RECONCILIAÇÃO
# ============================================================

def reconcile(
    collection,
    bronze_table,
    ingestion_id,
    start_time
):

    end_time = dt.datetime.utcnow()


    try:

        if spark.catalog.tableExists(
            bronze_table
        ):

            qtd_destino = (

                spark.table(
                    bronze_table
                )

                .filter(
                    F.col(
                        "_ingestion_id"
                    ) == ingestion_id
                )

                .count()
            )

        else:

            qtd_destino = 0


        duration = (
            end_time - start_time
        ).total_seconds()


        print(
            f"{collection}: "
            f"{qtd_destino} registros "
            f"na Bronze."
        )


        spark.sql(
            f"""
            UPDATE {CONTROL_TABLE}

            SET
                qtd_gravada_destino =
                    {qtd_destino},

                end_time =
                    TIMESTAMP(
                        '{end_time.isoformat()}'
                    ),

                duracao_seg =
                    {duration},

                status =
                    'SUCCESS'

            WHERE
                _ingestion_id =
                    '{ingestion_id}'
            """
        )


    except Exception as exc:

        print(
            f"[WARN] Falha na "
            f"reconciliação: {exc}"
        )


# ============================================================
# EXECUÇÃO
# ============================================================

setup_unity_catalog_objects()

create_control_table()


# ============================================================
# PROCESSAMENTO DAS COLEÇÕES
# ============================================================

for collection_cfg in COLLECTIONS:

    # --------------------------------------------------------
    # Coleção habilitada?
    # --------------------------------------------------------

    if not collection_cfg.get(
        "enabled",
        True
    ):

        continue


    collection = (
        collection_cfg["collection"]
    )


    load_type = (
        collection_cfg["modo_carga"]
    )


    destination = (
        collection_cfg["destino"]
    )


    bronze_table = (
        f"{CATALOG}."
        f"{BRONZE_SCHEMA}."
        f"{destination}"
    )


    ingestion_id = get_ingestion_id(
        collection
    )


    start_time = (
        dt.datetime.utcnow()
    )


    print("\n")

    print("#" * 80)

    print(
        f"COLLECTION: {collection}"
    )

    print(
        f"LOAD TYPE: {load_type}"
    )

    print(
        f"BRONZE: {bronze_table}"
    )

    print(
        f"INGESTION ID: {ingestion_id}"
    )

    print("#" * 80)


    try:

        # ----------------------------------------------------
        # FULL
        # ----------------------------------------------------

        if load_type == "full":

            load_full(
                collection_cfg,
                ingestion_id
            )


        # ----------------------------------------------------
        # INCREMENTAL
        # ----------------------------------------------------

        else:

            load_incremental(
                collection_cfg,
                ingestion_id
            )


        # ----------------------------------------------------
        # RECONCILIAÇÃO
        # ----------------------------------------------------

        reconcile(
            collection,
            bronze_table,
            ingestion_id,
            start_time
        )


    except Exception as exc:

        print(
            f"[ERRO] Bronze falhou "
            f"para {collection}: "
            f"{exc}"
        )


        end_time = (
            dt.datetime.utcnow()
        )


        duration = (
            end_time - start_time
        ).total_seconds()


        try:

            spark.sql(
                f"""
                UPDATE {CONTROL_TABLE}

                SET
                    end_time =
                        TIMESTAMP(
                            '{end_time.isoformat()}'
                        ),

                    duracao_seg =
                        {duration},

                    status =
                        'FAILED',

                    mensagem_erro =
                        '{str(exc).replace("'", "''")}'

                WHERE
                    _ingestion_id =
                        '{ingestion_id}'
                """
            )

        except Exception as control_error:

            print(
                f"[WARN] Falha ao "
                f"registrar erro: "
                f"{control_error}"
            )


        raise


# ============================================================
# FIM
# ============================================================

print("\n")

print("=" * 80)

print(
    "BRONZE JOB FINALIZADO COM SUCESSO"
)

print("=" * 80)