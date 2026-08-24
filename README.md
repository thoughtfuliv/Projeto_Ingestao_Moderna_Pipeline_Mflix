# Projeto Ingestão Moderna - Pipeline Mflix
Pipeline de ingestão de dados desenvolvida para o trabalho final da disciplina de **Engenharia de Dados / Ingestão Moderna de Dados**.

O projeto realiza a ingestão das coleções do banco `sample_mflix`, disponibilizado pelo MongoDB Atlas, e materializa os dados na camada **Bronze**  e **Silver** de um Data Lake utilizando **Databricks, PySpark e Delta Lake**.

A solução foi projetada para ser **genérica, parametrizada, incremental, rastreável e resiliente**, evitando a criação de código duplicado para cada coleção.

----------

## 📌 Objetivo

O objetivo do projeto é construir uma pipeline moderna de ingestão capaz de:

-   extrair dados do MongoDB Atlas;
    
-   ingerir múltiplas collections utilizando o mesmo código;
    
-   suportar cargas `full` e `incremental`;
    
-   controlar cargas incrementais por watermark;
    
-   realizar leitura em lotes;
    
-   utilizar projection na origem;
    
-   aplicar retry com backoff exponencial;
    
-   preservar a rastreabilidade de cada execução;
    
-   armazenar os dados na camada Bronze em formato Delta Lake;
    
-   tratar registros que não possam ser convertidos;
    
-   realizar reconciliação entre origem e destino;
    
-   garantir idempotência na carga incremental.
    

O trabalho exige que todas as collections sejam ingeridas utilizando um único componente genérico e parametrizado.

----------

# 🏗️ Arquitetura
A arquitetura foi dividida em **três jobs independentes**.

## 2.1. Ingestion Job

O `ingestion_job.py` é responsável exclusivamente pela extração do MongoDB e pela disponibilização dos dados na Landing Zone.

```
MongoDB Atlas
      │
      ▼
MongoReader
      │
      ├── Full / Incremental
      ├── Watermark
      ├── Batch
      ├── Projection
      └── Retry
      │
      ▼
Landing Zone
      │
      └── JSON/JSONL
```

O job **não grava diretamente na Bronze**.

----------

## 2.2. Bronze Job

O `bronze_job.py` é responsável exclusivamente por consumir os arquivos disponibilizados na Landing Zone.

Para coleções full (dimensões pequenas: users, theaters, sessions, embedded_movies), a Bronze é tratada como snapshot completo por execução via overwrite — cada execução substitui o snapshot anterior, mantendo fidelidade total à origem no momento da leitura.

Para coleções incrementais (movies, comments), a Bronze é estritamente append-only via MERGE ... WHEN NOT MATCHED THEN INSERT, nunca reescrevendo histórico.

```
Landing Zone
      │
      ▼
Databricks Auto Loader
      │
      ▼
readStream
      │
      ├── Schema Inference
      ├── schemaLocation
      ├── Checkpoint
      └── Schema Evolution
      │
      ▼
Bronze Delta
```
## 2.3. Silver Job
A preencher.


# 🗂️ Fonte de dados

A fonte utilizada é o banco `sample_mflix` do MongoDB Atlas.


----------

# 📁 Estrutura do projeto


```text
.
├── README.md              
│
├── config/
│   ├── pipeline_config.yaml      
│   └── collections.json          
│
├── jobs/
│   ├── ingestion_job.py
│   └── bronze_job.py
│
├── notebooks/
│   └── (seus notebooks de desenvolvimento e evidências)
│
├── docs/
│   ├── ARQUITETURA.md    
│   └── evidencias/        
│       ├── execucao_01_full_load.png
│       ├── execucao_02_incremental_sem_novidades.png
│       └── execucao_03_incremental_com_dados.png
│
└── CONTRIBUICOES.md     

```




