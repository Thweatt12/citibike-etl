CitiBike ETL Pipeline

This project builds a complete ETL pipeline for CitiBike NYC trip data, using modern data engineering tools like Apache Airflow, Docker, and PostgreSQL. The pipeline follows the Bronze → Silver → Gold layered data architecture for scalable and maintainable data workflows.

---

Project Structure
citibike-etl/
├── dags/
│ ├── bronze/
│ │ ├── unzip.py # Extract and unzip raw files
│ │ └── create_tables.sql # Create raw staging tables
│ ├── silver/
│ │ └── normalize_load.py # Normalize and clean raw data
│ ├── gold/
│ │ └── build_dims_facts.sql # Build final fact/dimension tables
│ ├── utils/
│ │ └── run_sql.py # Helper script to run SQL files
├── docker-compose.yaml # Docker services config (Airflow, DB, etc.)
├── Dockerfile # Airflow image
├── .gitignore
└── README.md # This file


---

## Technologies Used

- **Apache Airflow** – Task orchestration and scheduling
- **PostgreSQL** – Data storage (data warehouse)
- **Python** – ETL and normalization logic
- **Docker** – Containerized development environment

---

 DAG Overview

The pipeline is managed by an Airflow DAG called `citibike_data_pipeline`. It runs through the following sequence of tasks:

1. **Unzip** – Extracts raw data files
2. **Create Tables** – Creates raw staging tables in the database
3. **Normalize Load** – Cleans and standardizes data (Silver layer)
4. **Build Facts** – Creates final analytical tables (Gold layer)

Each step corresponds to a layer in the pipeline:

| Layer   | Description |
|---------|-------------|
| Bronze  | Raw data ingestion and storage |
| Silver  | Data cleaning and normalization |
| Gold    | Final business-ready tables |

---

## Running the Project

### Prerequisites

- Docker
- Docker Compose

### Steps

1. Clone the repository:
   ```bash
   git clone https://github.com/Thweatt12/citibike-etl.git
   cd citibike-etl
