# Walmart Sales ETL Pipeline

##  Overview
This project implements a complete **ETL pipeline** for Walmart sales data using **Apache Airflow**.

The pipeline is designed as a portfolio project to demonstrate practical **data engineering skills**:
- Building modular ETL workflows  
- Orchestrating jobs with **Airflow**  
- Persisting data in **PostgreSQL**  
- Containerizing with **Docker Compose**  
- Adding CI/CD automation with **GitHub Actions**  
- Visualizing metrics with **Grafana**  

---

## Architecture

**Data Flow:**

```mermaid
flowchart LR
    RAW[Raw Layer] --> STG[Staging Layer]
    STG --> PROCESSED[Processed Layer]
    PROCESSED --> METRICS[Metrics/KPIs]
```
- Raw Layer: Stores unprocessed, source data.
- Staging Layer (optional): Cleans, deduplicates, and enriches data for analytics. Can be implemented as an intermediate table (stg.orders) or handled in Python/SQL transformations before the processed layer.
- Processed Layer: Final business-ready tables, including calculated metrics and KPIs. This layer is ready for analytics, reporting, and dashboard consumption.
- Metrics Layer:Aggregated KPIs for dashboards and reporting. Typically stored in a separate table or materialized view to optimize queries for visualization tools like Grafana.


##  Project Structure
```
walmart-etl-pipeline/
├── airflow/                         # Airflow components
│   ├── dags/                        # DAG definitions
│   │   ├── __pycache__/             # Compiled Python files
│   │   └── dag_walmart.py
│   ├── etl/                         # Python ETL modules
│   │   ├── __init__.py
│   │   ├── extract.py
│   │   ├── transform.py
│   │   ├── load.py
│   │   ├── metrics.py
│   │   └── quality.py
│   ├── data/                        # Source CSV files
│   │   └── walmart.csv
│   └── tests/                       # Unit and integration tests
│       ├── data_quality.py
│       └── test_etl.py
├── sql/                             # SQL scripts
│   ├── transform.sql                # Transformations for processed layer
│   └── metrics.sql                  # KPI / aggregate calculations
├── db/                              # Database initialization scripts
│   └── init.sql
├── grafana/                         # Dashboards setup
├── docs/                            # Documentation and demos
│   └── screenshots/
│       ├── airflow_ui.png
│       └── grafana_dashboard.png
├── requirements-airflow.txt         # Python dependencies for Airflow
├── docker-compose.yml               # Service orchestration
├── Dockerfile                       # Custom Airflow image
└── README.md                        # Project documentation

```

---

##  Tech Stack
- **Apache Airflow** – orchestration  
- **PostgreSQL** – database  
- **Docker / Docker Compose** – containerization  
- **GitHub Actions** – CI/CD  
- **Pytest** – testing  
- **Grafana** – visualization

---

##  Getting Started

### 1. Clone repository
```bash
git clone https://github.com/dravindel/ecommerce-etl.git
cd ecommerce-etl
```

### 2. Create `.env` file
```env
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
AIRFLOW__CORE__FERNET_KEY=YOUR_KEY
```

### 3. Start services
```bash
docker-compose up --build -d
```

Access after startup:
- Airflow UI → [http://localhost:8080](http://localhost:8080)  
- Grafana (optional) → [http://localhost:3000](http://localhost:3000)  

---

##  ETL Pipeline

Steps in the DAG:
1. **Extract** → Load raw data (CSV → PostgreSQL).  
2. **Data Quality** → Validate input.  
3. **Load Raw** → Store unprocessed data.  
4. **Transform** → Clean & aggregate.  
5. **Load Processed** → Persist transformed data.  
6. **Metrics** → Calculate KPIs.  

DAG flow:
```mermaid
flowchart TD
    A[Extract] --> B[Data Quality]
    B --> C[Load Raw]
    C --> D[Transform]
    D --> E[Load Processed]
    E --> F[Calculate Metrics]
```

---

##  Tests
Run unit tests locally:
```bash
pytest airflow/tests/ --disable-warnings -q
```

---

##  CI/CD
- **CI (ci.yaml):** runs linting (flake8) and tests (pytest).  
- **CD (cd.yaml):** builds & pushes Airflow Docker image.  

To enable CD, add the following secrets in your GitHub repo:
- `DOCKER_USERNAME`  
- `DOCKER_PASSWORD`  

---

## Demo illustration

### Airflow DAG
![Airflow DAG](docs/screenshots/airflow_ui.png)

### Grafana Dashboard
![Grafana Dashboard](docs/screenshots/grafana_dashboard.png)

---

## Author
**👤 [Dravindel](https://www.linkedin.com/in/dmitrylakhov)**  
Portfolio project for **Data Engineering interviews demonstrations**.

