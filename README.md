# Projeto Ingestão Moderna — Pipeline Mflix

Pipeline do MongoDB Atlas para Databricks, desenvolvido para a disciplina de Engenharia de Dados / Ingestão Moderna de Dados. As seis coleções do banco `sample_mflix` são processadas por componentes genéricos e parametrizados, com cargas `full` ou `incremental`.

## Arquitetura

```text
MongoDB Atlas
  -> ingestion_job.py (projection, batch, watermark e retry)
  -> Landing Zone (JSON Lines em Unity Catalog Volume)
  -> bronze_job.py (Auto Loader, schema e checkpoint)
  -> Delta Bronze (append-only e particionado por data)
  -> silver_job.py (validação, deduplicação, hash e MERGE)
  -> Delta Silver + control_quality_log
```

| Componente | Responsabilidade |
|---|---|
| `notebooks/ingestion_job.py` | Extrair do MongoDB e gravar JSONL na Landing |
| `notebooks/bronze_job.py` | Ingerir arquivos com Auto Loader e adicionar metadados técnicos |
| `notebooks/silver_job.py` | Validar, deduplicar, normalizar e materializar a Silver |
| `config/collections.json` | Definir coleções, modo, watermark, destino e projection |
| `config/pipeline_config.yaml` | Centralizar catálogo, schemas, volumes e parâmetros técnicos |

## Coleções

| Coleção | Modo | Watermark | Projection |
|---|---|---|---|
| `users` | full | — | exclui `password` |
| `theaters` | full | — | — |
| `sessions` | full | — | exclui `jwt` |
| `embedded_movies` | full | — | exclui `plot_embedding` |
| `movies` | incremental | `lastupdated` | exclui `fullplot` |
| `comments` | incremental | `date` | — |

## Técnicas adotadas

### 1. Leitura em lotes

O cursor do PyMongo usa `batch_size`. Os documentos são consumidos progressivamente e agrupados em arquivos JSONL de até `BATCH_SIZE`, limitando memória e reduzindo a criação de *small files*.

A leitura não é particionada por faixas de `_id`: as coleções são processadas sequencialmente, escolha adequada ao volume do Mflix e aos limites da Free Edition.

### 2. Projection na origem

A configuração `projecao` é enviada ao `find` do MongoDB. Campos desnecessários ou sensíveis são descartados antes da transferência, reduzindo rede, memória, serialização e armazenamento.

### 3. Paralelismo e particionamento

Uma coleção é extraída por vez, evitando excesso de conexões simultâneas. Na Bronze, os dados são particionados por `_ingestion_date`, atributo de baixa cardinalidade. Checkpoints separados por coleção impedem o reprocessamento de arquivos já confirmados.

### 4. Connection pooling

Cada coleção cria um `MongoClient`, reutilizado pelo cursor, lotes e retries daquela coleção. O pool interno do PyMongo permanece ativo durante esse ciclo e o cliente é fechado em `finally`.

O código ainda não compartilha um único cliente entre coleções nem configura `maxPoolSize`; essas são otimizações futuras.

## Confiabilidade e qualidade

### Incremental e retry

`movies` e `comments` usam watermark persistida em `control_watermark`. O controle só avança após a gravação dos arquivos. Falhas transitórias são repetidas até `max_retries`, com backoff `2 ** tentativa`.

### Tratamento de schema drift

A Bronze combina schema explícito por coleção com `schema_evolution_mode: rescue`. Campos fora do contrato são preservados em `_rescued_data`. Na Silver, registros com dados resgatados são filtrados e não seguem para as tabelas de negócio.

`schemaLocation` e `checkpointLocation` são isolados por coleção. As escritas Delta usam `mergeSchema`, e os `MERGE` da Silver usam `withSchemaEvolution()`.

### Metadados e idempotência

A Bronze acrescenta `_ingestion_id`, `_ingestion_timestamp`, `_source_path`, `_source_collection`, `_load_type`, `_ingestion_date` e `_source_id`.

A Silver mantém a versão mais recente por `_source_id`, calcula `_record_hash` e executa `MERGE`. Registros sem chave, corrompidos ou com rescued data são filtrados antes da materialização.

### Reconciliação

Cada coleção gera uma linha em `silver.control_quality_log` com:

- `source_count`: registros lidos da Bronze;
- `valid_count`: válidos após deduplicação;
- `null_key_count` e `null_key_pct`;
- `duplicate_key_count`: grupos de `_source_id` duplicados na Bronze válida;
- duração, status e mensagem de erro.

A Silver não recebe `_source_id` nulo; essas linhas são filtradas antes do processamento.

Os status registrados pelo controle de qualidade são:

- `SUCCESS`: execução sem exceção e com percentual de chaves nulas dentro do limiar;
- `PARTIAL`: percentual de chaves nulas acima do limiar configurado;
- `FAILED`: exceção durante o processamento.

O controle atual não mede `destination_count`, divergência origem × destino, contagem por lote nem reconciliação acumulada. A duplicidade é calculada sobre toda a Bronze válida, não apenas sobre o lote atual.

## Decisões de confiabilidade

- **Carga full e incremental:** o modo é parametrizado por coleção.
- **Watermark:** cargas incrementais consultam registros posteriores ao último ponto confirmado.
- **Retry com backoff exponencial:** falhas transitórias são repetidas com espera crescente.
- **Auto Loader:** descobre arquivos e mantém progresso por checkpoint.
- **Schema explícito e rescued data:** dados inesperados são preservados em `_rescued_data`.
- **Idempotência Silver:** `_source_id`, hash do registro e `MERGE` evitam duplicações.

- **Observabilidade:** logs registram contagens, duração, watermark e status.

## Configuração

Parâmetros principais em `config/pipeline_config.yaml`:

```yaml
pipeline:
  catalog: "meu_catalog"
  landing_schema: "landing"
  checkpoints_schema: "checkpoints"
  schemas_schema: "schemas"
  bronze_schema: "bronze"
  silver_schema: "silver"
  volume_name: "mflix"
  batch_size: 5000
  max_retries: 3
  infer_column_types: true
  schema_evolution_mode: "rescue"
  available_now: true
```
A URI do MongoDB é obtida pelo secret scope `conn-db`, chave `cnn-mongodb-sampleflix`, e não fica no repositório.

## Estrutura

```text
.
├── README.md
├── CONTRIBUICOES.md
├── config/
│   ├── collections.json
│   └── pipeline_config.yaml
├── docs/
│   └── ARQUITETURA.md
└── notebooks/
    ├── ingestion_job.py
    ├── bronze_job.py
    └── silver_job.py
```

## Ordem de execução

1. Executar `ingestion_job.py` para extrair dados para a Landing Zone.
2. Executar `bronze_job.py` para materializar as tabelas Bronze.
3. Executar `silver_job.py` para validar e materializar as tabelas Silver.

Os notebooks devem ser executados no Git Folder do projeto, usando compute Serverless e com acesso ao catálogo e aos volumes configurados.
