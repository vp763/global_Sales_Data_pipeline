# 🌍 Global Multi-Source Sales Analytics Pipeline (Airflow + PySpark + Medallion Architecture)

An end-to-end data engineering pipeline orchestrated using **Dockerized Apache Airflow** and processed at scale with **PySpark**. It ingests data from multiple transactional and analytical systems, runs transformations through a **Medallion Architecture (Bronze ➡️ Silver ➡️ Gold)**, and models the final output into an enterprise-ready **Star Schema** formatted for Cloud Data Warehouses (e.g., Microsoft Fabric, Snowflake, BigQuery).

---

## 🏗️ Architecture & Data Flow

- **Source Ingestion (Bronze Layer):**
  - **PostgreSQL (OLTP):** Live customer orders database.
  - **Forex REST API:** Real-time multi-currency exchange rates relative to USD (`open.er-api.com`).
  - **CRM Master File:** Customer demographics, tiers, and credit risk score (CSV).
- **Cleansing & Typing (Silver Layer - PySpark):**
  - Deduplicated customer master data.
  - Filtered refunded orders (`order_status != 'REFUNDED'`).
  - Strict type casting and validated risk scores.
  - Saved as columnar **Parquet** files.
- **Dimensional Modeling (Gold Layer - PySpark):**
  - Converted multi-currency transactions into a unified `USD` figure using forex rates.
  - Modeled into a **Star Schema**:
    - `Fact_Sales` (Keys + Numerical Transaction Metrics)
    - `Dim_Customer` (Customer Attributes & Risk Metadata)

---

## 📂 Project Structure

```text
global_sales/
├── dags/
│   └── global_sales_analytics/
│       └── dag_global_sales_ingestion.py   # Airflow DAG defining Bronze-Silver-Gold pipeline
├── data/
│   └── global_sales/
│       ├── sourcecrm/
│       │   └── customer_crm.csv            # Master CRM flat file
│       ├── bronze/                         # Raw ingested JSON/CSV files
│       ├── silver/                         # Cleaned Parquet tables
│       └── gold/                           # Final analytical Parquet tables
├── Dockerfile                              # Custom image with Java 17 & PySpark
├── docker-compose.yaml                     # Airflow 2.10.2 multi-container orchestration
├── requirements.txt                        # Python dependencies
├── .gitignore
└── README.md
```

---

## 🛠️ Tech Stack

- **Orchestrator:** Apache Airflow 2.10.2
- **Processing Engine:** Apache Spark (PySpark 3.5.1) with OpenJDK 17
- **Database:** PostgreSQL 13 (OLTP & Metadata)
- **Container Engine:** Docker & Docker Compose
- **Storage Formats:** Apache Parquet, JSON, CSV

---

## 🚀 Setup & Execution Guide

### 1. Configure Environment Variables
Create a `.env` file in the `global_sales` directory:
```bash
echo -e "AIRFLOW_UID=50000\nFERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqJZPo=" > .env
```

### 2. Build and Start Airflow Services
Build the PySpark-ready image and start containers:
```bash
docker compose up -d --build
```

Verify all containers are up and healthy:
```bash
docker compose ps
```

### 3. Seed Mock OLTP Database (PostgreSQL)
Run the following command to populate sample multi-currency order records inside the Postgres container:
```bash
docker compose exec postgres psql -U airflow -d airflow -c "
CREATE TABLE IF NOT EXISTS customer_orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INT,
    order_amount NUMERIC(10, 2),
    currency VARCHAR(5),
    order_timestamp TIMESTAMP,
    order_status VARCHAR(20)
);

INSERT INTO customer_orders VALUES
('ORD_1001', 'CUST_01', 'PROD_99', 2, 140.00, 'USD', '2026-10-14 10:15:00', 'COMPLETED'),
('ORD_1002', 'CUST_02', 'PROD_88', 1, 8500.00, 'INR', '2026-10-14 10:30:00', 'COMPLETED'),
('ORD_1003', 'CUST_03', 'PROD_77', 4, 320.00, 'EUR', '2026-10-14 11:00:00', 'COMPLETED'),
('ORD_1004', 'CUST_01', 'PROD_88', 1, 110.00, 'USD', '2026-10-14 11:45:00', 'COMPLETED'),
('ORD_1005', 'CUST_04', 'PROD_66', 3, 250.00, 'GBP', '2026-10-14 12:20:00', 'COMPLETED'),
('ORD_1006', 'CUST_05', 'PROD_99', 5, 350.00, 'USD', '2026-10-14 12:50:00', 'REFUNDED')
ON CONFLICT (order_id) DO NOTHING;
"
```

### 4. Trigger Pipeline in Airflow
1. Navigate to `http://localhost:8080`.
2. Login with credentials: `airflow` / `airflow`.
3. Locate DAG: `global_sales_ingestion`.
4. Turn the toggle **ON** and click the **Trigger (Play icon)**.

---

## 📊 Pipeline Output Verification

After task completion, check the partitioned parquet storage:
- **Bronze Zone:** `data/global_sales/bronze/`
- **Silver Zone:** `data/global_sales/silver/` (`orders.parquet`, `customers.parquet`, `forex_rates.parquet`)
- **Gold Zone:** `data/global_sales/gold/` (`fact_sales.parquet`, `dim_customer.parquet`)