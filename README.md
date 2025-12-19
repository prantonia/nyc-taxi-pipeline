# NYC Taxi Data Pipeline

[![NYC Taxi Pipeline CI/CD](https://github.com/prantonia/nyc-taxi-pipeline/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/prantonia/nyc-taxi-pipeline/actions/workflows/ci.yml)
![Python Version](https://img.shields.io/badge/python-3.12-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)

A production-grade data engineering pipeline for processing NYC Taxi trip data using Python orchestration, Google BigQuery, and PyArrow optimization. Implements a medallion architecture (Bronze/Silver/Gold) with comprehensive testing, automated CI/CD, and intelligent idempotency checks.

---

## **Table of Contents**

- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Pipeline Modes](#pipeline-modes)
- [Testing](#testing)
- [CI/CD](#cicd)
- [Documentation](#documentation)

---

## **Features**

### **Core Capabilities**
- **Medallion Architecture** - Bronze (Raw), Silver (Transformed), Gold (Aggregated) layers
- **PyArrow Optimization** - 3-5x faster data processing for large datasets
- **Intelligent Idempotency** - Row-based checking prevents duplicate processing
- **Dual Pipeline Modes** - Full refresh and incremental loads
- **Automatic Progression** - Incremental pipeline automatically moves through months
- **Retry Logic** - Exponential backoff for failed operations
- **Comprehensive Logging** - Structured logging with no print statements
- **Metadata Tracking** - Complete audit trail of all pipeline runs
- **Extensive Testing** - Unit and integration tests with >80% coverage
- **CI/CD Automation** - GitHub Actions with automated testing and linting

### **Technical Highlights**
- Direct BigQuery loading without intermediate file storage
- Batch processing to prevent memory overflow
- Smart date range handling for data infiltrations
- Production-grade error handling and recovery

---

## **Architecture**

![Pipeline Architecture](assets/architecture.svg)

**Key Components:**
- **Python Orchestrator** - Coordinates all pipeline operations
- **BigQuery Client** - Handles all database interactions
- **Data Loader** - Downloads and loads data with PyArrow
- **Metadata Manager** - Tracks pipeline execution history
- **Retry Handler** - Implements exponential backoff logic
---

## **Quick Start**

### **Prerequisites**
- Python 3.12
- Google Cloud Platform account
- BigQuery dataset created
- Service account with BigQuery permissions

### **5-Minute Setup**

```bash
# 1. Clone repository
git clone https://github.com/prantonia/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure credentials
cp service-account-key-template.json service-account-key.json
# Edit service-account-key.json with your GCP credentials

# 5. Update configuration
# Edit .env with your project ID and dataset name using the .env.example file

# 6. Create tables
python -c "from src.orchestrator import PipelineOrchestrator; \
           orch = PipelineOrchestrator(); orch.create_tables()"

# 7. Run pipeline
python run_full_refresh.py

# Done! Check your BigQuery tables
```

---

## **Installation**

### **Detailed Installation**

1. **Clone the Repository**
   ```bash
   git clone https://github.com/prantonia/nyc-taxi-pipeline.git
   cd nyc-taxi-pipeline
   ```

2. **Set Up Python Environment**
   ```bash
   # Create virtual environment
   python3.12 -m venv venv

   # Activate (Linux/Mac)
   source venv/bin/activate

   # Activate (Windows)
   venv\Scripts\activate
   ```

3. **Install Dependencies**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. **Configure Google Cloud**
   ```bash
   # Create service account on GCP
   # Download JSON key file
   # Save as service-account-key.json in project root

   # Update src/config.py:
   PROJECT_ID = "your-project-id"
   DATASET_ID = "your_dataset"
   ```

5. **Initialize Database**
   ```bash
   python -c "from src.orchestrator import PipelineOrchestrator; \
              orch = PipelineOrchestrator(); orch.create_tables()"
   ```

---

## **Usage**

### **Full Refresh Pipeline**

Loads all 12 months of 2024 data at once:

```bash
python run_full_refresh.py
```

**Expected output:**
```
[STEP 1/4] Loading data to Staging layer
Processing January 2024 (1/12)
Downloaded 2,964,624 rows for January 2024
January uploaded. Total uploaded: 2,964,624 rows
...
All months processed. Total uploaded: 41,169,720 rows

[STEP 2/4] Loading data to Raw layer
Raw table recreated with 35,428,594 rows

[STEP 3/4] Transforming data to Silver layer
Silver table created with 30,124,885 rows

[STEP 4/4] Aggregating data to Gold layer
Gold table created with 409 rows

FULL REFRESH PIPELINE COMPLETED SUCCESSFULLY
Runtime: 1,245.67 seconds
```

### **Incremental Pipeline**

Loads data month by month, automatically progressing:

```bash
# Load next month automatically
python run_incremental.py

# Or specify a month
python run_incremental.py --month 3
```

**Auto-progression example:**
```
Run 1: Loads January
Run 2: Skips January (exists), loads February
Run 3: Skips February (exists), loads March
...
Run 12: Loads December
Run 13: All months loaded, pipeline completes
```

---

## **Project Structure**

```
nyc-taxi-pipeline/
│
├── src/                        # Source code
│   ├── __init__.py
│   ├── bigquery_client.py     # BigQuery operations with PyArrow
│   ├── config.py              # Configuration constants
│   ├── data_loader.py         # Download and load data
│   ├── metadata_manager.py    # Pipeline execution tracking
│   ├── orchestrator.py        # Main pipeline orchestrator
│   └── retry_handler.py       # Retry logic with backoff
│
├── sql/                        # SQL scripts
│   ├── create_staging_table.sql
│   ├── create_raw_table.sql
│   ├── create_silver_table.sql
│   ├── create_gold_table.sql
│   └── create_metadata_table.sql
│
├── tests/                      # Test suite
│   ├── test_Bigquery_client.py
│   ├── test_config.py
│   ├── test_data_loader.py
│   ├── test_metadata.py
│   ├── test_orchestrator.py
│   └── test_retry_handler.py
│
├── .github/workflows/          # CI/CD
│   └── ci.yml
│
├── docs/                       # Documentation
│   ├── data_dictionary.md
│   └── project_documentation.md
│
├── README.md
├── requirements.txt
├── run_full_refresh.py
└── run_incremental.py
```

---

## **Pipeline Modes**

| Mode | Use Case | Duration |
|------|----------|----------|
| **Full Refresh** | Initial load, complete refresh | ~20 min |
| **Incremental** | Add new months, regular updates | ~5 min |

---

## **Testing**

```bash
# Run all tests
pytest tests/ -v --cov=src --cov-report=term-missing

# Linting
flake8 src/ tests/

# Code formatting
black src/ tests/
```

---

## **CI/CD**

Automated checks on every push:
- Linting (flake8)
- Unit tests (pytest)
- Code coverage
- Code formatting (black)

See [Project_documentation](docs/project_documentation.md) for details.

---

## **Documentation**

| Document | Description |
|----------|-------------|
| [project_documentation](docs/project_documentation.md) | Detailed Project Documentation |
| [data_dictionary](docs/data_dictionary.md) | Data definitions |

---

## **Author**

- GitHub: [@prantonia](https://github.com/prantonia)

---

#### If you find this project useful, please give it a star⭐!
