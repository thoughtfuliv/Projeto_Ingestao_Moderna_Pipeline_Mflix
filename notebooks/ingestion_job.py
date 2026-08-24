# Responsabilidade deste job:
# 1. Ler dados do MongoDB
# 2. Aplicar carga full ou incremental
# 3. Usar watermark quando configurado
# 4. Exportar documentos como JSON Lines para a Landing Zone
#
# IMPORTANTE:
# Este job NÃO grava na Bronze.
# A Bronze é responsabilidade exclusiva do bronze_job.py.

import datetime as dt
import json
import time
import uuid

import bson
import yaml
from pymongo import MongoClient


# ============================================================
# CONFIGURAÇÃO
# ============================================================

CONFIG_PATH = "/Workspace/Repos/<usuario>/Projeto_Ingestao_Moderna_Pipeline_Mflix/config/pipeline_config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

PIPELINE = CONFIG["pipeline"]

LANDING_BASE_PATH = PIPELINE["landing_base_path"]
BATCH_SIZE = int(PIPELINE.get("batch_size", 5000))
MAX_RETRIES = int(PIPELINE.get("max_retries", 3))


# ============================================================
# MONGODB
# ============================================================

MONGODB_URI = dbutils.secrets.get(
    scope="conn-db",
    key="cnn-mongodb-sampleflix",
)


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


# ============================================================
# WATERMARK
# ============================================================

# O watermark fica em uma tabela Delta de controle.
# O bronze_job.py também usa essa mesma tabela.

CATALOG = PIPELINE["catalog"]
BRONZE_SCHEMA = PIPELINE["bronze_schema"]

WATERMARK_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.control_watermark"
)


def get_watermark(collection):
    """
    Busca o último watermark processado.

    Se ainda não existir tabela/registro, retorna None.
    """

    try:
        if not spark.catalog.tableExists(WATERMARK_TABLE):
            return None

        row = (
            spark.table(WATERMARK_TABLE)
            .filter(f"collection = '{collection}'")
            .orderBy("updated_at", ascending=False)
            .first()
        )

        if row is None:
            return None

        return row["watermark_value"]

    except Exception:
        return None


# ============================================================
# FILTRO MONGODB
# ============================================================

def build_filter(collection_cfg, watermark):
    """
    Monta o filtro incremental.

    Full:
        {}

    Incremental:
        campo_watermark > último watermark
    """

    if collection_cfg["modo_carga"] != "incremental":
        return {}

    campo = collection_cfg.get("campo_watermark")

    if not campo or not watermark:
        return {}

    # comments.date normalmente é armazenado como Date no MongoDB.
    if collection_cfg["collection"] == "comments":
        return {
            campo: {
                "$gt": parse_watermark(watermark)
            }
        }

    # Para campos armazenados como string.
    return {
        campo: {
            "$gt": watermark
        }
    }


# ============================================================
# WATERMARK DO LOTE
# ============================================================

def get_nested_value(document, field):
    """Busca campo simples ou nested usando dot notation."""

    value = document

    for part in field.split("."):
        if not isinstance(value, dict):
            return None

        value = value.get(part)

    return value


def update_watermark_candidate(
    current,
    document,
    field,
):
    """Obtém o maior watermark encontrado nos documentos."""

    if not field:
        return current

    value = get_nested_value(document, field)

    if value is None:
        return current

    if isinstance(value, (dt.datetime, dt.date)):
        value = value.isoformat()

    value = str(value)

    if current is None or value > current:
        return value

    return current


# ============================================================
# EXPORTAÇÃO
# ============================================================

def export_collection(collection_cfg):
    """
    MongoDB -> Landing Zone.

    Cada lote gera um novo arquivo JSON Lines.

    Exemplo:

    landing/
      movies/
        movies__UUID__000000.json
        movies__UUID__000001.json
    """

    database = collection_cfg["database"]
    collection = collection_cfg["collection"]

    projection = collection_cfg.get("projecao") or {}

    watermark_initial = get_watermark(collection)

    mongo_filter = build_filter(
        collection_cfg,
        watermark_initial,
    )

    client = MongoClient(
        MONGODB_URI,
        serverSelectionTimeoutMS=15000,
        socketTimeoutMS=300000,
        appName="mflix-modern-ingestion",
    )

    export_id = str(uuid.uuid4())

    collection_path = (
        f"{LANDING_BASE_PATH.rstrip('/')}/{collection}"
    )

    dbutils.fs.mkdirs(collection_path)

    total = 0
    batch = []
    batch_number = 0

    watermark_final = watermark_initial

    print("=" * 80)
    print(f"Collection: {collection}")
    print(f"Modo: {collection_cfg['modo_carga']}")
    print(f"Watermark inicial: {watermark_initial}")
    print(f"Filtro: {mongo_filter}")
    print("=" * 80)

    tentativa = 0

    try:
        while True:

            try:
                cursor = client[database][collection].find(
                    filter=mongo_filter,
                    projection=projection,
                    batch_size=BATCH_SIZE,
                )

                for document in cursor:

                    # Atualiza candidato a watermark
                    watermark_final = update_watermark_candidate(
                        watermark_final,
                        document,
                        collection_cfg.get("campo_watermark"),
                    )

                    # Documento JSON original.
                    # Não criamos uma coluna "body".
                    body = json.dumps(
                        document,
                        default=encode_value,
                        ensure_ascii=False,
                    )

                    batch.append(body)

                    if len(batch) >= BATCH_SIZE:

                        file_path = (
                            f"{collection_path}/"
                            f"{collection}__"
                            f"{export_id}__"
                            f"{batch_number:06d}.json"
                        )

                        # JSON Lines:
                        # uma linha = um documento MongoDB.
                        dbutils.fs.put(
                            file_path,
                            "\n".join(batch),
                            overwrite=False,
                        )

                        print(
                            f"Landing criada: {file_path} | "
                            f"registros: {len(batch)}"
                        )

                        total += len(batch)
                        batch = []
                        batch_number += 1

                cursor.close()

                break

            except Exception as exc:

                tentativa += 1

                if tentativa > MAX_RETRIES:
                    raise

                espera = 2 ** tentativa

                print(
                    f"Erro na leitura de {collection}: {exc}. "
                    f"Tentativa {tentativa}/{MAX_RETRIES}. "
                    f"Aguardando {espera}s..."
                )

                time.sleep(espera)

        # Último lote
        if batch:

            file_path = (
                f"{collection_path}/"
                f"{collection}__"
                f"{export_id}__"
                f"{batch_number:06d}.json"
            )

            dbutils.fs.put(
                file_path,
                "\n".join(batch),
                overwrite=False,
            )

            print(
                f"Landing criada: {file_path} | "
                f"registros: {len(batch)}"
            )

            total += len(batch)

    finally:
        client.close()

    print("=" * 80)
    print(f"Exportação finalizada: {collection}")
    print(f"Total exportado: {total}")
    print(f"Watermark final: {watermark_final}")
    print("=" * 80)

    return {
        "collection": collection,
        "export_id": export_id,
        "qtd_exportada": total,
        "watermark_inicial": watermark_initial,
        "watermark_final": watermark_final,
    }


# ============================================================
# EXECUÇÃO
# ============================================================

for collection_cfg in CONFIG["collections"]:

    if not collection_cfg.get("enabled", True):
        continue

    export_collection(collection_cfg)
