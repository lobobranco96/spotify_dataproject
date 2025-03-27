# 🎵 Pipeline ETL com API do Spotify, Airflow e Spark

Este repositório contém uma pipeline de dados para coletar, transformar e armazenar dados da API do Spotify utilizando **Apache Airflow** como orquestrador, **Apache Spark** para processamento distribuído e **MinIO** como Data Lake. O ambiente é totalmente **dockerizado**, garantindo facilidade na configuração e escalabilidade.

## 🏗️ Arquitetura

1️⃣ **Coleta de dados**: Airflow executa uma DAG que faz requisições à API do Spotify.  
2️⃣ **Armazenamento inicial (Raw Layer)**: Os dados coletados são armazenados em um bucket no MinIO.  
3️⃣ **Processamento com Spark**: Airflow dispara um job Spark via `SparkSubmitOperator` para transformar os dados.  
4️⃣ **Armazenamento Processado (Processed Layer)**: Os dados transformados são carregados em outro bucket no MinIO.

## 🛠️ Tecnologias Utilizadas

- **Apache Airflow** → Orquestração das tarefas  
- **Apache Spark** → Processamento distribuído  
- **MinIO** → Armazenamento de dados (Data Lake)  
- **Docker** → Gerenciamento do ambiente  
- **Spotify API** → Fonte dos dados

## 📂 Estrutura do Projeto

```bash
├── docker/
│   ├── Spark/
│   │   └── Dockerfile
│   ├── Airflow/
│   │   ├── Dockerfile
│   │   └── requirements.txt
├── include/
│   └── imgs/                 # Imagens e diagramas do projeto
├── mnt/
│   ├── airflow/
│   │   ├── dags/
│   │   │   ├── etl_pipeline.py
│   │   │   ├── python/
│   │   │   │   ├── config.py
│   │   │   │   ├── extract/
│   │   │   │   │   ├── fetch_spotify_data.py
│   │   │   │   │   ├── __init__.py
│   │   ├── logs/
│   │   ├── plugins/
│   │   ├── config/
│   ├── minio/
│   │   ├── raw/
│   │   ├── processed/
│   ├── spark_job/
│   │   ├── code/
│   │   │   ├── __init__.py
│   │   │   ├── parquet_writer.py
│   │   │   ├── spark_session.py
│   │   │   ├── spotify_transformation.py
│   │   ├── jars/
│   │   ├── data_transformation.py
│   ├── services/
│   │   ├── orchestration.yml
│   │   ├── processing.yml
│   │   ├── storage.yml
│   │   ├── conf/
│   │   │   └── credentials.conf
├── README.md                  # Documentação do projeto
└── .env                       # Configurações sensíveis (API Keys, URLs, etc.)

## 🚀 Como Executar

1️⃣ **Clone o repositório**:

```bash
git clone https://github.com/seu-usuario/etl-spotify-airflow.git
cd etl-spotify-airflow

2️⃣ Configure as variáveis de ambiente

Crie um arquivo .env na raiz do projeto e adicione:

SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password
MINIO_DOMAIN=storage
MINIO_REGION_NAME=us-east-1
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password
S3_ENDPOINT=http://minio:9000

3️⃣ Suba o ambiente Docker

docker compose -f services/orchestration.yml up -d --build
docker compose -f services/processing.yml up -d --build
docker compose -f services/storage.yml up -d 

Isso iniciará os serviços do Airflow, Spark e MinIO.

4️⃣ Acesse a interface do Airflow

Abra http://localhost:8080 e ative a DAG etl_pipeline.

5️⃣ Verifique os dados no MinIO

Acesse http://localhost:9000 com as credenciais padrão (minioadmin/minioadmin) para verificar os buckets raw e processed.

📊 Fluxo de Execução

1️⃣ Ingestão: A DAG do Airflow coleta dados da API do Spotify e salva no MinIO (raw/)
2️⃣ Transformação: Spark processa os dados do raw/, aplicando limpeza e estruturação.
3️⃣ Carga: Dados processados são armazenados no bucket processed/.

## 📌 Próximos Passos

- **Carregar os dados em um Banco de dados**: Após o processamento, os dados podem ser carregados em um banco de dados relacional ou NoSQL para consultas e visualização.
- **Criar um dashboard para visualização dos dados**: Um dashboard pode ser construído com ferramentas como Power BI, Tableau ou até mesmo uma aplicação customizada para apresentar os dados de forma visual.

## 💡 Contribuições

Contribuições são bem-vindas! Se você tiver alguma sugestão, melhorias ou correções, sinta-se à vontade para abrir uma **issue** ou enviar um **Pull Request (PR)**. Seu feedback é importante para melhorar o projeto!

## 📩 Contato

## 📚 Licença

Este projeto está licenciado sob a [MIT License](LICENSE).

---

Obrigado por conferir o projeto! 🚀
