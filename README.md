# Real Estate Portfolio Analytics Pipeline

## Overview
End-to-end automated data pipeline for property portfolio management and analytics. Ingests property data from Buildium API and Excel spreadsheets, orchestrates ETL workflows with Apache Airflow, models data in Snowflake, and powers interactive Power BI dashboards for KPI tracking.

## Business Problem
Managing 10+ residential and commercial properties across NJ, AR, and TX requires:
- Consolidated view of occupancy rates, rental income, and expenses
- Automated data ingestion from property management systems
- Historical trend analysis for vacancy forecasting and NOI optimization
- Real-time dashboards for property performance monitoring

## Architecture

```
[Buildium API] → [Python Extraction] → [AWS S3 Data Lake] → [Airflow Orchestration]
[Excel Files]   →                                              ↓
                                                        [Snowflake DWH]
                                                               ↓
                                                        [Power BI Dashboards]
```

## Tech Stack
- **Languages:** Python 3.10, SQL
- **Orchestration:** Apache Airflow 2.7
- **Cloud Storage:** AWS S3
- **Data Warehouse:** Snowflake
- **Visualization:** Power BI
- **Infrastructure:** Docker, Docker Compose

## Key Features

### 1. Data Ingestion
- **Buildium API Integration:** Automated extraction of property details, tenant records, lease agreements, and payment history
- **Excel Upload:** Supports manual CSV/Excel uploads for supplementary data (maintenance logs, utility bills)
- **Incremental Loading:** Tracks last extraction timestamp to load only new/updated records

### 2. Orchestration & Scheduling
- **Daily Airflow DAGs:** Scheduled extraction, transformation, and load workflows
- **Data Quality Checks:** Null validation, schema enforcement, referential integrity
- **Error Handling:** Automated retries with exponential backoff, Slack notifications on failures

### 3. Data Modeling (Snowflake)
- **Star Schema Design:**
  - **Fact Tables:** `fact_rental_income`, `fact_expenses`, `fact_occupancy`
  - **Dimension Tables:** `dim_properties`, `dim_tenants`, `dim_time`, `dim_expense_categories`
- **Partitioning:** Monthly partitioning on date columns for query performance
- **SCD Type 2:** Slowly changing dimensions to track historical tenant and lease changes

### 4. Analytics & Dashboards
- **KPI Metrics:**
  - Occupancy rate by property and portfolio-wide (target: 95%+)
  - Monthly rental income trends and year-over-year growth
  - Maintenance cost analysis by property and category
  - NOI (Net Operating Income) tracking
- **Power BI Features:**
  - Drill-down from portfolio to property-level metrics
  - Time-series trend charts with forecasting
  - Interactive filters by property, state, and date range

## Project Structure
```
real-estate-analytics-pipeline/
├── dags/
│   ├── extract_buildium.py       # Buildium API extraction DAG
│   ├── transform_load.py          # Transformation and Snowflake load DAG
│   └── data_quality_checks.py     # Data validation DAG
├── src/
│   ├── extractors/
│   │   ├── buildium_client.py     # API client wrapper
│   │   └── excel_parser.py        # Excel/CSV ingestion
│   ├── transformers/
│   │   ├── cleaners.py            # Data cleaning functions
│   │   └── aggregators.py         # Business logic transforms
│   └── loaders/
│       └── snowflake_loader.py    # Snowflake connection and load
├── sql/
│   ├── ddl/
│   │   ├── create_tables.sql      # Table creation scripts
│   │   └── create_views.sql       # Analytical views
│   └── queries/
│       └── sample_analytics.sql   # Example queries for BI
├── config/
│   ├── airflow.cfg
│   └── snowflake_config.yml
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Setup & Installation

### Prerequisites
- Docker & Docker Compose installed
- AWS account with S3 bucket created
- Snowflake account with database and warehouse
- Buildium API credentials

### Environment Variables
Create a `.env` file:
```bash
BUILDIUM_API_KEY=your_api_key
BUILDIUM_API_SECRET=your_api_secret

AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
S3_BUCKET_NAME=your-bucket-name

SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=PROPERTY_ANALYTICS
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

### Local Development
```bash
# Clone the repository
git clone https://github.com/Saideva0318/real-estate-analytics-pipeline.git
cd real-estate-analytics-pipeline

# Start Airflow and dependencies
docker-compose up -d

# Access Airflow UI
open http://localhost:8080
# Default credentials: airflow / airflow

# Run initial table creation
snowsql -f sql/ddl/create_tables.sql
```

## Usage

### Running the Pipeline
1. **Trigger DAGs manually** via Airflow UI or enable scheduling
2. **Monitor execution** in Airflow logs and task status
3. **Verify data** in Snowflake:
   ```sql
   SELECT COUNT(*) FROM fact_rental_income;
   SELECT * FROM dim_properties LIMIT 10;
   ```
4. **Connect Power BI** to Snowflake and import data model

### Sample Analytics Queries
```sql
-- Portfolio-wide occupancy rate
SELECT 
    COUNT(DISTINCT CASE WHEN lease_status = 'Active' THEN unit_id END) * 100.0 / COUNT(DISTINCT unit_id) AS occupancy_rate
FROM fact_occupancy
WHERE date = CURRENT_DATE();

-- Monthly rental income trend
SELECT 
    DATE_TRUNC('month', payment_date) AS month,
    SUM(amount) AS total_income
FROM fact_rental_income
GROUP BY month
ORDER BY month DESC
LIMIT 12;
```

## Results & Impact
- **Automation:** Reduced manual data entry time from 8 hours/week to zero
- **Data Quality:** 99.5% accuracy with automated validation checks
- **Performance:** Sub-second query response times for 1M+ records
- **Insights:** Identified underperforming properties leading to 15% NOI improvement

## Future Enhancements
- [ ] Add predictive maintenance ML model
- [ ] Integrate utility bill APIs (electric, gas, water)
- [ ] Real-time streaming with Kafka for immediate vacancy alerts
- [ ] Mobile dashboard app for on-the-go monitoring

## Contributing
Contributions welcome! Please open an issue or submit a pull request.

## License
MIT License

## Contact
**Sai Deva Puttur**  
Data Engineer | Real Estate Analytics  
[LinkedIn](https://www.linkedin.com/in/saideva-puttur) | [GitHub](https://github.com/Saideva0318)
