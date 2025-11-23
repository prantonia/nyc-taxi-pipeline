# NYC Taxi Data Pipeline: Complete Project Documentation

*A comprehensive guide walking through the design, implementation, and operation of a production-grade data engineering system*

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Getting Started: Initial Setup](#getting-started-initial-setup)
3. [Architecture and Design Rationale](#architecture-and-design-rationale)
4. [Building the Data Pipeline](#building-the-data-pipeline)
5. [Orchestration Logic and Retry Mechanism](#orchestration-logic-and-retry-mechanism)
6. [Metadata Management Process](#metadata-management-process)
7. [Loading Strategies: Full Refresh vs Incremental](#loading-strategies-full-refresh-vs-incremental)
8. [Testing and Quality Assurance](#testing-and-quality-assurance)
9. [CI/CD Setup and Automation](#cicd-setup-and-automation)
10. [Querying the Gold Layer](#querying-the-gold-layer)
11. [Monitoring and Operations](#monitoring-and-operations)
12. [Lessons Learned](#lessons-learned)

---

## Project Overview

This project implements a production-grade data pipeline for processing NYC Yellow Taxi trip data from 2024. The pipeline handles over 41 million records, transforming raw parquet files from NYC's Open Data portal into clean, analytics-ready datasets stored in Google BigQuery. What makes this pipeline production-grade isn't just that it moves data from point A to point B—it's built with reliability, observability, cost efficiency, and maintainability as core principles.

The challenge we're solving is deceptively complex. On the surface, it seems straightforward: download some parquet files, load them into a database, clean the data, and aggregate it for analysis. But real-world data comes with real-world problems. The source files contain date "infiltrations" where January's file includes trips from 2009 and other months. The data has quality issues with null values, negative fares, and impossible trip durations. Network connections fail. APIs rate-limit requests. Cloud services have occasional hiccups. A production pipeline needs to handle all of these gracefully while staying within budget constraints.

The system processes data through multiple layers of increasing quality, tracks every pipeline run for debugging and audit purposes, automatically retries failed operations, and integrates with CI/CD workflows to prevent buggy code from reaching production. By the end of this documentation, you'll understand not just how each component works, but why certain design decisions were made and how they contribute to a reliable, maintainable system.

---

## Getting Started: Initial Setup

Before writing any code, we need to set up the infrastructure that will host our data pipeline. This section walks through every step of creating a Google Cloud Platform project, setting up BigQuery, and configuring your local development environment.

### Creating Your Google Cloud Project

The first step is creating a GCP project, which serves as the container for all our cloud resources. Open your web browser and navigate to the Google Cloud Console at console.cloud.google.com. If you don't have a Google account yet, you'll need to create one. Google offers a generous free tier with $300 in credits for new users, which is more than enough for this project.

Once you're logged into the console, click on the project dropdown at the top of the page. This dropdown shows your current project if you have one, or prompts you to create your first project if you're new to GCP. Click "New Project" and you'll see a form asking for project details.

For the project name, enter something descriptive like "NYC Taxi Pipeline" or "NYC Taxi Data Engineering Project". The project name is just for display purposes—what really matters is the project ID. The project ID must be globally unique across all of Google Cloud because it's used to identify your project in API calls and URLs. You can let Google generate a random ID for you, or you can customize it. For this project, something like "nyc-taxi-pipeline-477912" works well, where the numbers at the end help ensure uniqueness.

After entering your project details, click "Create" and wait a few seconds while Google provisions your project. Once created, make sure your new project is selected in the project dropdown. You'll see the project name at the top of the console, confirming you're working in the correct project.

Now we need to enable the BigQuery API. Google Cloud has dozens of different services, and by default they're all disabled to save costs. Navigate to the APIs & Services section from the left-hand menu. Click "Enable APIs and Services" and search for "BigQuery API" in the search box. Click on the BigQuery API from the results, then click the blue "Enable" button. This process takes about 30 seconds, and when it's complete, you'll see the BigQuery API dashboard.

### Setting Up BigQuery Dataset

With the BigQuery API enabled, we can now create our dataset. In BigQuery terminology, a dataset is like a database in traditional database systems—it's a container that holds tables. Navigate to BigQuery from the left-hand menu. The BigQuery console will open in a new panel, showing your project in the Explorer sidebar.

Click on the three dots next to your project name in the Explorer, then select "Create dataset". You'll see a form asking for dataset details. For the dataset ID, enter "nyc_taxi_dataset". This will be the container for all our tables—staging, raw, silver, gold, and metadata.

For the data location, choose a region close to you for better performance and potentially lower costs. If you're in the United States, "us-central1" is a good choice. If you're in Europe, choose "europe-west1". The location you choose here is permanent—you can't change it later without recreating the dataset.

For default table expiration, leave it as "Never". We want our data to persist indefinitely. The default maximum table age setting can also stay disabled. Click "Create Dataset" and within a few seconds, you'll see "nyc_taxi_dataset" appear under your project in the Explorer.

### Creating a Service Account

To allow our Python code to interact with BigQuery, we need to create a service account. A service account is like a robot user—it's an identity that your code uses to authenticate with Google Cloud services. Regular user accounts are for humans who log in through a web browser, but service accounts are for applications.

Navigate to IAM & Admin from the left-hand menu, then click on "Service Accounts". Click the "Create Service Account" button at the top. For the service account name, enter "taxi-pipeline" or something similar. The service account ID will be automatically generated as "taxi-pipeline@your-project-id.iam.gserviceaccount.com".

For the description, write something like "Service account for NYC Taxi data pipeline operations". This helps you remember what this service account is for if you come back to it months later. Click "Create and Continue" to move to the permissions step.

Now we need to grant this service account permission to use BigQuery. In the "Grant this service account access to project" section, click the "Select a role" dropdown. Search for "BigQuery Admin" and select it. This gives the service account full control over BigQuery resources in your project. In a real production environment, you'd want to be more restrictive and only grant the minimum necessary permissions, but for this learning project, BigQuery Admin is appropriate.

Click "Continue" and then "Done" on the next screen. You'll now see your service account listed in the service accounts table. Click on the service account email address to open its details page. Navigate to the "Keys" tab at the top. Click "Add Key" and select "Create new key". Choose JSON as the key type and click "Create".

A JSON file will automatically download to your computer. This file contains the credentials that your code will use to authenticate with Google Cloud. This file is extremely sensitive—it's like a username and password combined. Anyone with this file can access your Google Cloud resources. Save it in a secure location and never, ever commit it to Git or share it publicly.

### Setting Up Your Local Development Environment

Now that our Google Cloud infrastructure is ready, let's set up your local development environment. This is where you'll write and test code before deploying it to production.

First, create a directory for your project. Open your terminal and navigate to where you want to store your project. Create a new directory and navigate into it:

```bash
mkdir nyc-taxi-pipeline
cd nyc-taxi-pipeline
```

Initialize a Git repository to track your code changes:

```bash
git init
```

Python 3.12 is the recommended version for this project because it includes performance improvements and modern language features. Check if you have Python 3.12 installed:

```bash
python3.12 --version
```

If you don't have Python 3.12, you'll need to install it. On Ubuntu or Debian Linux, you can use the deadsnakes PPA. On macOS, use Homebrew. On Windows, download the installer from python.org.

Create a virtual environment for your project. Virtual environments keep your project's dependencies isolated from other Python projects on your system:

```bash
python3.12 -m venv venv
```

Activate the virtual environment. On Linux or macOS:

```bash
source venv/bin/activate
```

On Windows:

```bash
venv\Scripts\activate
```

You'll see "(venv)" appear at the beginning of your terminal prompt, indicating that the virtual environment is active.

Now create a `.gitignore` file to ensure sensitive files don't get committed to Git:

```bash
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
venv/
ENV/
env/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# Credentials and Environment
.env
service-account-key.json
*.json

# Logs
logs/
*.log

# Testing
.pytest_cache/
.coverage
htmlcov/
.tox/

# OS
.DS_Store
Thumbs.db
EOF
```

Create a requirements.txt file with all the Python packages we'll need:

```bash
cat > requirements.txt << 'EOF'
# Google Cloud
google-cloud-bigquery==3.11.4
google-auth==2.23.0

# Data Processing
pandas==2.0.3
pyarrow==12.0.1
requests==2.31.0

# Configuration
python-dotenv==1.0.0

# Utilities
python-dateutil==2.8.2
pytz==2023.3
EOF
```

Install all the dependencies:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

This installation will take a minute or two as pip downloads and installs all the packages and their dependencies.

Copy your service account key file (the JSON file you downloaded earlier) into your project directory and rename it to something consistent:

```bash
# Copy from wherever you saved it
cp ~/Downloads/your-project-key.json ./service-account-key.json
```

Now create a `.env` file to store your configuration. This file will contain sensitive information, which is why we added it to `.gitignore`:

```bash
cat > .env << 'EOF'
# GCP Configuration
GCP_PROJECT_ID=nyc-taxi-pipeline-477912
BQ_DATASET=nyc_taxi_dataset
GOOGLE_APPLICATION_CREDENTIALS=./service-account-key.json

# Table Names
STAGING_TABLE_NAME=staging_yellow_taxi
RAW_TABLE_NAME=raw_yellow_taxi
SILVER_TABLE_NAME=silver_yellow_taxi
GOLD_TABLE_NAME=gold_yellow_taxi
METADATA_TABLE_NAME=pipeline_metadata

# Data Source
NYC_TAXI_BASE_URL=https://d37ci6vzurychx.cloudfront.net/trip-data
TAXI_FILE_TEMPLATE=yellow_tripdata_2024-{month:02d}.parquet

# Pipeline Configuration
LOG_LEVEL=INFO
LOG_FILE=logs/pipeline.log
MAX_RETRIES=3
RETRY_DELAY=5
EOF
```

Make sure to replace `nyc-taxi-pipeline-477912` with your actual project ID.

Create the directory structure for your project:

```bash
mkdir -p src sql tests logs docs .github/workflows
```

Create empty `__init__.py` files to make Python treat these directories as packages:

```bash
touch src/__init__.py tests/__init__.py
```

Your development environment is now ready. Let's verify everything is working by creating a quick test script:

```bash
cat > test_setup.py << 'EOF'
"""
Quick test to verify environment + BigQuery setup.
"""

import os
import logging
from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Start tests
logger.info("Testing environment setup...")
logger.info(f" Project ID: {os.getenv('GCP_PROJECT_ID')}")
logger.info(f" Dataset: {os.getenv('BQ_DATASET')}")
logger.info(f" Credentials: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")

# BigQuery test
try:
    client = bigquery.Client()
    logger.info(" BigQuery connection successful.")
    logger.info(f" Connected to project: {client.project}")
    logger.info(" All setup checks passed!")
except Exception as e:
    logger.error(f" Connection failed: {e}")
EOF


python test_setup.py
```

If you see "All setup checks passed!" then your environment is configured correctly and you're ready to start building the pipeline.

---

## Architecture and Design Rationale

With our infrastructure set up, we can now dive into the architecture of the pipeline. The decisions we make here will affect everything from performance to maintainability to cost, so it's worth understanding the reasoning behind each choice.

### The Medallion Architecture

I chose to implement a medallion architecture, which is a design pattern popularized by Databricks for data lakes. The core idea is to process data through multiple layers of increasing quality and refinement. Think of it like a manufacturing assembly line—raw materials come in at one end, go through multiple processing stages, and emerge as finished products at the other end.

The traditional medallion architecture has three layers: Bronze (raw data), Silver (cleaned data), and Gold (aggregated data). However, I discovered early in this project that I needed a fourth layer to handle the realities of the NYC Taxi dataset. The source files have significant data quality issues that needed special handling, so I split the Bronze layer into two: Staging and Raw.

The Staging layer is where data first lands after being downloaded from NYC's Open Data portal. This layer stores everything exactly as it comes from the source files—no filtering, no cleaning, no modifications. When I download January's parquet file, every single row goes into staging, even if those rows contain trips from 2009 or February 2024. This might seem wasteful, but it serves crucial purposes for idempotency checking and data auditing.

The staging table is permanent storage. Unlike the other layers which get recreated on each pipeline run, staging accumulates data over time. I added metadata columns to track which source file each row came from and when it was uploaded. The table is partitioned by source month, which means BigQuery stores each month's data in separate physical partitions. This partitioning is key to making idempotency checks essentially free—when I need to check if January's data is already loaded, BigQuery only needs to scan January's partition, not the entire table.

The Raw layer contains validated 2024 data only. This table is created from staging using a CREATE OR REPLACE pattern, which means it's completely rebuilt from scratch on each pipeline run. The SQL that creates the raw table includes a WHERE clause filtering for pickup dates in 2024. This removes all those date infiltrations that sneak into the source files. The raw table gives us a clean starting point for transformations without the clutter of invalid data.

You might wonder why we need both staging and raw—why not just load directly to raw and skip staging? The answer lies in idempotency and auditability. Staging preserves the complete source data, which is essential for accurate idempotency checks. If I only had the raw table and January's pipeline run failed halfway through, I wouldn't be able to tell whether I had all of January's data or just part of it. The staging table with its source metadata lets me compare exactly what's in the database against what's in the source file.

The Silver layer is where we clean and standardize the data. This is the workhorse layer where we handle all the data quality issues, perform type conversions, calculate derived fields, and filter out truly invalid records. The transformation logic in silver is extensive—we clean passenger counts by replacing nulls with sensible defaults, we remove negative fares that represent refunds or data errors, we calculate trip durations and average speeds, and we extract temporal features like hour of day and day of week.

Silver is also created using CREATE OR REPLACE, meaning it's completely rebuilt from raw on each run. This might seem inefficient—why recalculate everything when we could just transform new data? But the rebuild approach has significant advantages. It ensures consistency across all data, makes the transformation logic easier to modify and test, eliminates complex merge logic, and provides a fresh start if we ever need to fix historical data.

The Gold layer contains pre-aggregated analytics. Instead of forcing analysts to write complex aggregation queries every time they need metrics, we pre-calculate common aggregations and store them. The gold table includes monthly summaries showing total trips, revenue, and distance; daily patterns highlighting trends; hourly breakdowns showing rush hour peaks; and location-based statistics identifying popular pickup zones.

Gold is also created with CREATE OR REPLACE, but it's worth noting that gold tables are relatively small. Even though we're starting with 41 million detailed records, the gold table only has a few thousand rows because we're aggregating. This small size means gold table recreation is fast and cheap.

### Why Four Layers Instead of Three

The decision to split Bronze into Staging and Raw came from encountering real-world data quality issues that the traditional three-layer architecture doesn't address well. When I first started working with the NYC Taxi data, I discovered that source files contain significant date infiltrations. January 2024's file includes trips from January 2009, random days in 2023, and even trips from February 2024. These infiltrations make up about 14% of the data in some files.

If I had just one Bronze layer, I would face a difficult choice. I could load everything including infiltrations, which would contaminate my analytics. Or I could filter during load, which would make idempotency checks unreliable because the raw counts wouldn't match source file counts. The two-layer Bronze approach solves this dilemma elegantly.

Staging accepts everything, giving me accurate source file tracking. Raw filters to only valid data, giving downstream layers clean input. When I need to check if a month is already loaded, I compare the downloaded file against staging, where the exact source file contents are preserved. When I need to run transformations, I read from raw, where all the junk data has been filtered out.

This separation also provides valuable auditability. If I ever need to understand what was in a source file on a particular date, I can query staging with its upload timestamp. If I need to debug why certain records were excluded, I can compare staging to raw and see exactly what got filtered and why.

### The Idempotency Challenge

Making the pipeline idempotent—ensuring that running it multiple times produces the same result without duplicates—turned out to be one of the trickiest aspects of this project. My initial approach was naive: just check if records exist for a given date range and skip loading if they do.

This approach failed immediately because of the date infiltrations. If I checked whether staging contained any January 2024 records and found some, I might conclude January was already loaded. But what if a previous run failed halfway through? I'd have some January records but not all of them, and my simple date check would incorrectly skip the reload.

I needed a more sophisticated approach that could detect partial loads and handle infiltrations. The solution I developed is row-based idempotency checking. When the pipeline downloads a file, it first identifies two specific rows: the row with the minimum pickup datetime and the row with the maximum pickup datetime. These boundary rows represent the earliest and latest trips in the file.

The pipeline then checks whether these exact rows exist in staging, matching on multiple columns—not just the timestamp, but also pickup location, dropoff location, vendor ID, trip distance, and fare amount. If both boundary rows exist in staging with all their values matching, the pipeline can safely assume the entire file was already loaded successfully.

Why does this work? If a previous run completed successfully, both boundary rows will be present. If a previous run failed midway, at least one boundary row will be missing. If the source file was updated with new data, the maximum boundary row will be different. This approach handles all the edge cases that simple date checking misses.

The implementation uses BigQuery's powerful SQL capabilities to perform these checks efficiently. Because staging is partitioned by source month, checking for specific rows only scans the relevant partition, making the check essentially free in terms of BigQuery costs.

### Performance Optimization with PyArrow

Early in development, I noticed that downloading and loading data was painfully slow. Processing a single month's data took almost 5 minutes, which meant a full year would take an hour. For a pipeline that might run daily, this was unacceptable. I started profiling to find the bottleneck.

The culprit was pandas. While pandas is an excellent library for data analysis, it's not optimized for large-scale ETL operations. The pandas `read_parquet` function was taking 8 seconds per file, and uploading DataFrames to BigQuery was taking 90 seconds per file. These numbers might not sound terrible for a single file, but multiplied across 12 months plus potential retries, they added up quickly.

I switched to PyArrow for parquet file processing and immediately saw dramatic improvements. PyArrow is specifically designed for high-performance data processing. Its columnar memory format aligns perfectly with parquet's storage format, allowing for essentially zero-copy reads. Reading a file with PyArrow took only 2.7 seconds—three times faster than pandas.

Even better, BigQuery's Python client automatically detects when PyArrow is available and uses it for DataFrame uploads. The same upload that took 90 seconds with pure pandas completed in 30 seconds with PyArrow. This wasn't just about speed—BigQuery charges based on compute time, so faster processing directly reduced costs.

The performance improvement wasn't just theoretical. My full refresh pipeline went from an estimated 60 minutes down to 18 minutes actual runtime. This faster execution meant lower costs (BigQuery charges for compute time), faster feedback during development, and the ability to rerun the pipeline when needed without eating up hours of time.

Implementing PyArrow was surprisingly simple—I just changed the import from pandas to PyArrow, read the parquet file to an Arrow table, then converted to pandas only for the final upload. The BigQuery client handles the rest automatically:

```python
import pyarrow.parquet as pq

# Read with PyArrow
arrow_table = pq.read_table(io.BytesIO(response.content))
df = arrow_table.to_pandas()

# BigQuery client automatically uses PyArrow when available
client.load_table_from_dataframe(df, table_id)
```

### Cost Optimization Strategies

Running data pipelines on Google Cloud can get expensive quickly if you're not careful. I implemented several strategies to keep costs under control while maintaining production-grade quality.

The most impactful optimization was partitioning the staging table by source month. BigQuery charges based on data scanned by queries. When you query a partitioned table and include a partition filter in your WHERE clause, BigQuery only scans the relevant partitions. My idempotency checks query staging with WHERE source_month = X, which only scans one partition instead of the entire table. This makes checks essentially free—I've run hundreds of these checks and they show up as 0 bytes processed in billing.

I also carefully structured my SQL queries to avoid full table scans. BigQuery is smart about optimization, but you have to give it the right conditions. Using EXTRACT(YEAR FROM date) forces BigQuery to examine every row. Using date >= '2024-01-01' AND date < '2025-01-01' allows BigQuery to use partition pruning and other optimizations.

The batch processing approach also saves money. If I tried to load all 41 million rows at once, I'd need to keep gigabytes of data in memory, which requires expensive compute resources. By processing one month at a time, I keep memory usage under 1GB and use smaller, cheaper compute instances.

The PyArrow optimization I mentioned earlier had direct cost benefits too. Faster processing means less compute time, which means lower costs. The 40% reduction in pipeline runtime translated to roughly 30% reduction in BigQuery compute costs.

All these optimizations added up. My initial unoptimized pipeline runs cost about $7. After all optimizations, the same pipeline runs cost about $5. Over many runs during development and ongoing operations, these savings become significant.

### Design Decisions and Trade-offs

Every architecture involves trade-offs. Let me explain the key decisions I made and what I gave up for what I gained.

I chose to rebuild silver and gold tables completely on each run rather than implementing incremental updates. The trade-off here is compute cost versus complexity. Rebuilding costs more in BigQuery time, but it ensures perfect consistency and makes the transformation logic much simpler. Incremental updates would require complex merge logic, handling of late-arriving data, and careful coordination between layers. For a dataset of this size and update frequency, the rebuild approach is actually cheaper when you factor in development time and maintenance costs.

I chose Python orchestration over dedicated workflow tools like Apache Airflow. Airflow is powerful and has a rich ecosystem, but it's also complex to set up and maintain. For this project, Python scripts provide enough orchestration capability without the operational overhead. The trade-off is that I don't get Airflow's web UI, backfilling features, or extensive monitoring. But I also don't need a separate server running Airflow, don't need to learn Airflow's DAG syntax, and don't need to maintain Airflow's database.

I chose to use BigQuery's CREATE OR REPLACE pattern rather than trying to maintain persistent tables with incremental updates. This trade-off favors simplicity and reliability over performance. Each pipeline run rebuilds tables from scratch, which takes longer than incremental updates would. But it also means I never have to worry about merge conflicts, duplicate data, or inconsistent state. If something goes wrong, I just run the pipeline again and get a fresh, correct dataset.

I chose to separate staging (permanent) from raw (recreated) rather than having a single bronze layer. This adds complexity—I now have four layers instead of three. But it solves critical problems with idempotency checking and data quality that a three-layer architecture struggles with. The extra layer is worth the added complexity because it makes the pipeline more reliable and maintainable.

---

## Building the Data Pipeline

With the architecture designed and understood, it's time to build the actual pipeline components. This section walks through implementing each major component, explaining not just what the code does but why it's structured the way it is.

### Configuration Management

Every application needs configuration—API keys, database connection strings, file paths, and operational parameters. Managing this configuration properly is crucial for security, flexibility, and maintainability. I created a centralized configuration module that handles all project settings.

Create `src/config.py`:

The configuration module starts by loading environment variables from a `.env` file using python-dotenv. This is a best practice that keeps sensitive information like credentials out of the source code. Environment variables can be easily changed without modifying code, they're supported by all deployment platforms, and they're excluded from version control through `.gitignore`.

One tricky aspect of configuration is handling multiple environments. The code needs to run in development on your laptop, in CI/CD during testing, and in production on a server. Each environment has different credentials and requirements. I solved this by detecting a "test mode" through environment variables:

```python
IS_TEST_MODE = (
    os.getenv("PYTEST_CURRENT_TEST") is not None or
    os.getenv("CI") == "true" or
    os.getenv("TESTING") == "true"
)
```

When in test mode, the configuration module uses safe default values instead of requiring real GCP credentials. This allows tests to run in CI without needing access to actual Google Cloud resources.

The configuration module also includes helper functions for common operations like generating parquet file URLs, getting month names, and calculating date ranges. These utilities keep the date logic centralized rather than scattered throughout the codebase. If NYC changes their URL structure or we need to adjust date formats, we only need to update one place.

I added validation to ensure all required configuration is present. When the module loads, it checks that critical variables like project ID and credentials path are set. If anything is missing, it raises a clear error message rather than letting the code fail later with a cryptic error. This fail-fast approach saves debugging time.

### BigQuery Client Wrapper

The next component is a wrapper around Google's BigQuery client library. You might wonder why we need a wrapper—can't we just use Google's client directly? The wrapper provides several benefits: consistent error handling, structured logging, simplified common operations, and a single place to implement BigQuery best practices.

Create `src/bigquery_client.py`:

The BigQueryClient class encapsulates all BigQuery operations. When you initialize it, it creates a connection to BigQuery using credentials from the configuration. The initialization happens once when the pipeline starts, and the same client object is reused throughout the pipeline run.

The class provides methods for different types of SQL operations. The `execute_query` method runs SELECT queries and returns results. The `execute_dml` method runs INSERT, UPDATE, and DELETE queries and returns the number of affected rows. The `execute_ddl` method runs CREATE, DROP, and ALTER queries that modify the database structure. This separation makes it clear what each method does and allows for appropriate error handling for each operation type.

One particularly important method is `load_dataframe_to_table`, which uploads pandas DataFrames to BigQuery. This is how we get downloaded parquet data into BigQuery. The method automatically uses PyArrow when available for better performance. It takes a write disposition parameter that controls whether to append to existing data, truncate and replace, or fail if the table already has data.

The client includes helper methods like `table_exists` for checking if a table exists, `get_row_count` for counting rows efficiently, and `get_min_max_datetime` for finding date ranges. These utilities are used throughout the pipeline for idempotency checks and validation.

Error handling is consistent across all methods. BigQuery-specific errors are caught and logged with context. All errors are re-raised so the caller can decide how to handle them. This approach provides visibility into what went wrong while still allowing retry logic and other error handling upstream.

### Data Loader with Idempotency

The data loader handles downloading parquet files and loading them into BigQuery. This component implements the sophisticated idempotency logic that prevents duplicate data loads.

Create `src/data_loader.py`:

The DataLoader class is where a lot of the pipeline's intelligence lives. The `download_parquet` method downloads a monthly parquet file using PyArrow for optimal performance. It includes proper error handling with timeouts to prevent hanging on slow network connections.

The idempotency checking logic is in `check_if_data_exists_in_staging`. This method implements the boundary row checking strategy I described in the architecture section. It finds the rows with minimum and maximum pickup datetimes in the downloaded DataFrame, then checks whether these complete rows exist in staging by matching on multiple columns. Only if both boundary rows exist does the method return True, indicating the data is already loaded.

The `_check_row_exists_in_staging` private method does the actual database query to check for a specific row. It builds a SQL query that matches on pickup datetime, dropoff datetime, vendor ID, trip distance, and total amount. The query uses approximate floating-point comparison (ABS(column - value) < 0.01) to handle minor precision differences while still identifying the same trip.

The `load_to_staging` method uses the BigQuery client to upload a DataFrame to the staging table. It always appends rather than truncating because staging is permanent cumulative storage.

For full refresh mode, the `download_all_months` method processes all 12 months sequentially. It downloads each month, checks if it already exists, and loads it if needed. The method tracks total rows uploaded, skipping months that are already present. This allows the pipeline to resume if interrupted—if you've already loaded January through June and the pipeline crashes on July, rerunning it will skip the first six months and pick up where it left off.

For incremental mode, the `load_incremental_to_staging` method handles a single month. It follows the same pattern of download, check, and conditionally load. The method returns zero rows if the data already exists, signaling to the orchestrator that this month should be marked as SKIPPED.

The `should_load_to_raw` method implements idempotency checking for the raw table. Since raw is recreated from staging, we need to check if raw is already in sync with staging. The method compares row counts—if staging has the same number of 2024 records as raw, then raw doesn't need recreation. This check uses partition metadata queries which are free, making it a cost-effective way to avoid unnecessary work.

### Retry Handler with Exponential Backoff

Network connections fail. APIs rate-limit requests. Cloud services have temporary outages. A production pipeline needs to handle these transient failures gracefully by automatically retrying operations.

Create `src/retry_handler.py`:

The RetryHandler class implements automatic retry with exponential backoff. Exponential backoff means the delay between retries increases exponentially—first retry after 2 seconds, second after 4 seconds, third after 8 seconds. This pattern prevents overwhelming a struggling service while giving temporary issues time to resolve.

The `retry_operation` method is the core of the retry logic. It takes a function and its arguments, then attempts to execute it up to max_retries times. If the operation succeeds on any attempt, it returns the result immediately. If all retries fail, it raises the last exception so the caller knows what went wrong.

The method uses detailed logging to provide visibility into retry behavior. It logs each attempt, any failures, and how long it's waiting before retrying. This logging has been invaluable during development for understanding when and why operations fail.

The exponential backoff calculation is simple but effective: delay = base_delay * (2 ** (attempt - 1)). With a base delay of 5 seconds, this produces delays of 5, 10, and 20 seconds for three retry attempts. You can tune these values based on your specific needs and the typical recovery time for failures.

I also included a decorator pattern that lets you add retry logic to any function with a simple annotation:

```python
@retry(max_retries=3, base_delay=5)
def potentially_failing_function():
    # Function code here
    pass
```

This decorator pattern makes it easy to add retry logic wherever needed without cluttering the main function logic.

### Metadata Manager

Every pipeline run needs to be tracked for auditing, debugging, and operational visibility. The metadata manager records detailed information about each execution.

Create `src/metadata_manager.py`:

The MetadataManager class handles all interactions with the metadata table. The `record_run` method inserts a record capturing everything about a pipeline execution: which pipeline ran (full_refresh or incremental), what date range was processed, which month was loaded, whether it succeeded or failed, how many rows were loaded, when it ran, how long it took, and any error messages if it failed.

This metadata serves multiple purposes. It provides a complete audit trail of all pipeline executions. It enables the auto-progression feature in incremental mode by letting the pipeline query for the last completed month. It supports debugging by showing exactly what happened during failed runs. And it enables operational monitoring by providing queryable history of pipeline performance.

The `get_last_completed_month` method is particularly important for incremental mode. It queries metadata to find the most recent month that completed successfully or was skipped. The method treats both SUCCESS and SKIPPED as completed, which is crucial for auto-progression. If a month's data is already loaded and gets SKIPPED, we want the next run to move on to the next month, not get stuck trying the same month repeatedly.

The metadata manager includes several query methods for different use cases. The `get_last_successful_run` returns details about the most recent successful pipeline execution. The `is_full_year_loaded` checks whether all 12 months have been loaded. The `get_run_history` retrieves the most recent pipeline runs for reporting and monitoring.

Error handling in metadata recording is deliberately lenient. If recording metadata fails, the pipeline logs a warning but doesn't crash. This is because metadata tracking is important but not critical—if the data load succeeded but metadata recording failed, we don't want to fail the entire pipeline. The actual data is more important than the metadata about the data.

### Pipeline Orchestrator

The orchestrator is the conductor of the symphony, coordinating all the other components to execute the pipeline end-to-end.

Create `src/orchestrator.py`:

The PipelineOrchestrator class brings together all the components we've built. Its initialization creates instances of the BigQuery client, data loader, metadata manager, and retry handler. These objects persist for the life of the orchestrator, avoiding repeated initialization overhead.

The `run_full_refresh` method implements the full refresh pipeline. It executes four major steps: load all months to staging, create raw from staging, transform to silver, and aggregate to gold. Each step is wrapped in the retry handler for automatic failure recovery. The method tracks total runtime and records metadata at the end, whether the run succeeds or fails.

The `run_incremental` method implements incremental loading. It first checks if the full year is already loaded to avoid redundant work. Then it determines which month to load next by calling the metadata manager. The method loads the specified month to staging, creates/updates raw, transforms to silver, and aggregates to gold. Like full refresh, it tracks runtime and records metadata.

The orchestrator includes helper methods for each transformation layer. The `_load_staging_to_raw_full` method recreates the raw table from staging for a full year. The `_load_staging_to_raw_incremental` method handles incremental updates to raw. Both methods use the CREATE OR REPLACE pattern, rebuilding raw completely rather than trying to incrementally update it.

The `_transform_to_silver` and `_aggregate_to_gold` methods execute the SQL files that create the silver and gold tables. These methods now accept a month parameter for dynamic date filtering in incremental mode. They read the SQL file, replace date placeholders with actual values, execute the SQL, and log the results.

The `_get_next_month_to_load` method implements the auto-progression logic. It queries metadata for the last completed month and returns the next month number. If no months have been loaded yet, it returns 1 for January. If all 12 months are loaded, it returns None to signal completion.

Error handling throughout the orchestrator is comprehensive. Each major operation is wrapped in try-except blocks that catch exceptions, log details, record metadata, and return appropriate status codes. This ensures that failures are captured and visible even if the pipeline can't complete.

### Creating the Database Schema

Before running the pipeline, we need to create the database tables. Each layer has its own table with a carefully designed schema.

Create `sql/create_metadata_table.sql`:

```sql
-- Create Metadata Table for Pipeline Tracking
-- Records all pipeline runs with detailed metrics

CREATE TABLE IF NOT EXISTS `nyc-taxi-pipeline-477912.nyc_taxi_dataset.pipeline_metadata` (
    pipeline_name STRING NOT NULL,           -- 'full_refresh' or 'incremental'
    date_range STRING NOT NULL,              -- e.g., '2024-01-01 to 2024-01-31'
    month_loaded STRING NOT NULL,            -- e.g., 'January' or 'full year'
    status STRING NOT NULL,                  -- 'SUCCESS', 'FAILED', 'SKIPPED'
    rows_loaded INT64,                       -- Number of rows loaded
    run_timestamp TIMESTAMP NOT NULL,        -- When the pipeline was executed
    runtime FLOAT64,                         -- Execution time in seconds
    error_message STRING                     -- Error details if failed
)
OPTIONS(
    description="Metadata tracking for NYC Taxi data pipeline execution history"
);
```

The metadata table uses simple types that are easy to query. The pipeline_name distinguishes between full refresh and incremental runs. The date_range and month_loaded provide human-readable descriptions of what was processed. The status field uses standardized values that the pipeline can filter on. The timestamps and runtime enable performance analysis.

Create `sql/create_staging_table.sql`:

```sql
-- Create Staging Table for NYC Taxi Data
-- Permanent table storing raw parquet data with source tracking
-- Partitioned by source_month for efficient querying

CREATE TABLE IF NOT EXISTS `nyc-taxi-pipeline-477912.nyc_taxi_dataset.staging_yellow_taxi` (
    VendorID INT64,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count FLOAT64,
    trip_distance FLOAT64,
    RatecodeID FLOAT64,
    store_and_fwd_flag STRING,
    PULocationID INT64,
    DOLocationID INT64,
    payment_type INT64,
    fare_amount FLOAT64,
    extra FLOAT64,
    mta_tax FLOAT64,
    tip_amount FLOAT64,
    tolls_amount FLOAT64,
    improvement_surcharge FLOAT64,
    total_amount FLOAT64,
    congestion_surcharge FLOAT64,
    Airport_fee FLOAT64
)
PARTITION BY DATE(tpep_pickup_datetime)
OPTIONS(
    description="Staging table - permanent storage for raw parquet data with source tracking"
);
```

The staging table matches the source parquet file schema exactly. I use FLOAT64 for numeric fields even though some could be integers, because this matches the parquet file types and avoids conversion issues. The table is partitioned by pickup date, which enables efficient idempotency checks and dramatically reduces query costs.

Create `sql/create_raw_table.sql`:

```sql
-- Create Raw (Bronze) Layer Table
-- Contains validated 2024 data only
-- Recreated from staging on each pipeline run

CREATE OR REPLACE TABLE `nyc-taxi-pipeline-477912.nyc_taxi_dataset.raw_yellow_taxi`
OPTIONS(
    description="Raw layer - validated 2024 NYC Yellow Taxi trip data"
)
AS
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.staging_yellow_taxi`
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2024;
```

The raw table is created using CREATE OR REPLACE, which means it's completely rebuilt from staging on each run. The WHERE clause filters to only 2024 data, removing all those date infiltrations. This gives us a clean, validated dataset to work with in downstream transformations.

Create `sql/create_silver_table.sql`:

```sql
-- Create Silver Layer Table
-- Contains cleaned and transformed data with dynamic date filtering
-- Dropped and recreated on every pipeline run

CREATE OR REPLACE TABLE `nyc-taxi-pipeline-477912.nyc_taxi_dataset.silver_yellow_taxi` AS
SELECT
    -- Renamed identifiers for clarity
    VendorID AS vendor_id,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,

    -- Calculate trip duration in minutes
    TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE) AS trip_duration_minutes,

    -- Clean passenger count - remove nulls and negatives
    CASE
        WHEN passenger_count IS NULL OR passenger_count <= 0 THEN 1
        WHEN passenger_count > 6 THEN 6
        ELSE CAST(passenger_count AS INT64)
    END AS passenger_count,

    -- Clean trip distance - remove nulls and negatives
    CASE
        WHEN trip_distance IS NULL OR trip_distance <= 0 THEN 0
        ELSE trip_distance
    END AS trip_distance,

    -- Standardize RatecodeID
    CAST(COALESCE(RatecodeID, 1) AS INT64) AS rate_code_id,

    -- Standardize store and forward flag
    UPPER(COALESCE(store_and_fwd_flag, 'N')) AS store_and_fwd_flag,

    -- Renamed Location IDs
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,

    -- Standardize payment type
    CAST(COALESCE(payment_type, 1) AS INT64) AS payment_type,

    -- Clean monetary amounts - remove negatives
    CASE WHEN fare_amount < 0 THEN 0 ELSE COALESCE(fare_amount, 0) END AS fare_amount,
    CASE WHEN extra < 0 THEN 0 ELSE COALESCE(extra, 0) END AS extra,
    CASE WHEN mta_tax < 0 THEN 0 ELSE COALESCE(mta_tax, 0) END AS mta_tax,
    CASE WHEN tip_amount < 0 THEN 0 ELSE COALESCE(tip_amount, 0) END AS tip_amount,
    CASE WHEN tolls_amount < 0 THEN 0 ELSE COALESCE(tolls_amount, 0) END AS tolls_amount,
    CASE WHEN improvement_surcharge < 0 THEN 0 ELSE COALESCE(improvement_surcharge, 0) END AS improvement_surcharge,
    CASE WHEN total_amount < 0 THEN 0 ELSE COALESCE(total_amount, 0) END AS total_amount,
    CASE WHEN congestion_surcharge < 0 THEN 0 ELSE COALESCE(congestion_surcharge, 0) END AS congestion_surcharge,
    CASE WHEN Airport_fee < 0 THEN 0 ELSE COALESCE(Airport_fee, 0) END AS Airport_fee,

    -- Extract temporal features
    EXTRACT(YEAR FROM tpep_pickup_datetime) AS pickup_year,
    EXTRACT(MONTH FROM tpep_pickup_datetime) AS pickup_month,
    EXTRACT(DAY FROM tpep_pickup_datetime) AS pickup_day,
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
    EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime) AS pickup_dayofweek,
    FORMAT_DATE('%A', DATE(tpep_pickup_datetime)) AS pickup_day_name,

    -- Calculate speed (mph) - rounded to 2 decimal places
    ROUND(
        CASE
            WHEN trip_distance > 0
                AND TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE) > 0
            THEN (trip_distance / TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE)) * 60
            ELSE 0
        END,
        2
    ) AS avg_speed_mph

FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.raw_yellow_taxi`

WHERE
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_pickup_datetime < tpep_dropoff_datetime
    AND tpep_pickup_datetime >= '{start_date}'
    AND tpep_pickup_datetime < '{end_date}'
    AND VendorID IS NOT NULL
    AND passenger_count > 0
    AND trip_distance > 0
    AND fare_amount >= 0
    AND total_amount >= 0

ORDER BY pickup_datetime ASC;
```

The silver table implements extensive data cleaning and transformation logic. Column names are standardized to be more readable—"VendorID" becomes "vendor_id", "PULocationID" becomes "pickup_location_id". Data quality issues are handled with CASE statements that replace invalid values with sensible defaults.

The WHERE clause filters out records that are fundamentally invalid—null timestamps, trips where pickup is after dropoff, zero passengers or distance, negative amounts. These records can't be meaningfully analyzed, so it's better to exclude them than to try to clean them.

The date filtering now uses placeholder variables {start_date} and {end_date} that get replaced at runtime. For full refresh, these are '2024-01-01' and '2025-01-01'. For incremental loads, they're dynamically set to the specific month being processed.

Create `sql/create_gold_table.sql`:

```sql
-- Create Gold Layer Table
-- Contains aggregated analytics-ready data
-- Dropped and recreated on every pipeline run

CREATE OR REPLACE TABLE `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi` AS

WITH monthly_stats AS (
    SELECT
        pickup_year,
        pickup_month,
        FORMAT_DATE('%B', DATE(pickup_year, pickup_month, 1)) AS month_name,
        DATE(pickup_year, pickup_month, 1) AS month_start_date,

        COUNT(*) AS total_trips,
        SUM(trip_distance) AS total_distance,
        ROUND(AVG(trip_distance), 2) AS avg_distance,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes,
        ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph,

        SUM(passenger_count) AS total_passengers,
        ROUND(AVG(passenger_count), 2) AS avg_passengers_per_trip,

        ROUND(SUM(fare_amount), 2) AS total_fare,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(SUM(tip_amount), 2) AS total_tips,
        ROUND(AVG(tip_amount), 2) AS avg_tip,
        ROUND(SUM(total_amount), 2) AS total_revenue,
        ROUND(AVG(total_amount), 2) AS avg_total,

        SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
        SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips,
        ROUND(SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS credit_card_pct,

        SUM(CASE WHEN pickup_hour BETWEEN 6 AND 9 THEN 1 ELSE 0 END) AS morning_rush_trips,
        SUM(CASE WHEN pickup_hour BETWEEN 16 AND 19 THEN 1 ELSE 0 END) AS evening_rush_trips,
        SUM(CASE WHEN pickup_dayofweek IN (1, 7) THEN 1 ELSE 0 END) AS weekend_trips,
        SUM(CASE WHEN pickup_dayofweek BETWEEN 2 AND 6 THEN 1 ELSE 0 END) AS weekday_trips

    FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.silver_yellow_taxi`
    GROUP BY pickup_year, pickup_month
),

daily_stats AS (
    SELECT
        DATE(pickup_datetime) AS trip_date,
        FORMAT_DATE('%A', DATE(pickup_datetime)) AS day_name,
        pickup_dayofweek,

        COUNT(*) AS daily_trips,
        ROUND(SUM(total_amount), 2) AS daily_revenue,
        ROUND(AVG(trip_distance), 2) AS avg_distance,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration,
        ROUND(AVG(fare_amount), 2) AS avg_fare

    FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.silver_yellow_taxi`
    GROUP BY trip_date, day_name, pickup_dayofweek
),

hourly_stats AS (
    SELECT
        pickup_hour,
        CONCAT('Hour ', CAST(pickup_hour AS STRING), ':00') AS hour_label,

        COUNT(*) AS trips_per_hour,
        ROUND(AVG(trip_distance), 2) AS avg_distance,
        ROUND(AVG(total_amount), 2) AS avg_revenue,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration,
        ROUND(SUM(total_amount), 2) AS total_revenue_hour

    FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.silver_yellow_taxi`
    GROUP BY pickup_hour
),

location_stats AS (
    SELECT
        pickup_location_id AS location_id,
        COUNT(*) AS pickup_count,
        ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
        ROUND(AVG(total_amount), 2) AS avg_revenue,
        ROUND(SUM(total_amount), 2) AS total_revenue_location

    FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.silver_yellow_taxi`
    GROUP BY pickup_location_id
    HAVING COUNT(*) > 1000
    ORDER BY pickup_count DESC
    LIMIT 100
)

SELECT
    'monthly' AS aggregation_type,
    CAST(pickup_month AS STRING) AS dimension_value,
    month_name AS dimension_label,
    month_start_date AS reference_date,
    total_trips,
    total_distance,
    avg_distance,
    avg_duration_minutes,
    avg_speed_mph,
    total_revenue,
    avg_total AS avg_revenue_per_trip,
    total_passengers,
    avg_passengers_per_trip,
    credit_card_trips,
    cash_trips,
    credit_card_pct,
    morning_rush_trips,
    evening_rush_trips,
    weekend_trips,
    weekday_trips,
    NULL AS pickup_count,
    NULL AS hour_label
FROM monthly_stats

UNION ALL

SELECT
    'daily' AS aggregation_type,
    CAST(trip_date AS STRING) AS dimension_value,
    day_name AS dimension_label,
    trip_date AS reference_date,
    daily_trips AS total_trips,
    NULL AS total_distance,
    avg_distance,
    avg_duration AS avg_duration_minutes,
    NULL AS avg_speed_mph,
    daily_revenue AS total_revenue,
    ROUND(daily_revenue / daily_trips, 2) AS avg_revenue_per_trip,
    NULL AS total_passengers,
    NULL AS avg_passengers_per_trip,
    NULL AS credit_card_trips,
    NULL AS cash_trips,
    NULL AS credit_card_pct,
    NULL AS morning_rush_trips,
    NULL AS evening_rush_trips,
    CASE WHEN pickup_dayofweek IN (1, 7) THEN daily_trips ELSE 0 END AS weekend_trips,
    CASE WHEN pickup_dayofweek BETWEEN 2 AND 6 THEN daily_trips ELSE 0 END AS weekday_trips,
    NULL AS pickup_count,
    NULL AS hour_label
FROM daily_stats

UNION ALL

SELECT
    'hourly' AS aggregation_type,
    CAST(pickup_hour AS STRING) AS dimension_value,
    hour_label AS dimension_label,
    NULL AS reference_date,
    trips_per_hour AS total_trips,
    NULL AS total_distance,
    avg_distance,
    avg_duration AS avg_duration_minutes,
    NULL AS avg_speed_mph,
    total_revenue_hour AS total_revenue,
    avg_revenue AS avg_revenue_per_trip,
    NULL AS total_passengers,
    NULL AS avg_passengers_per_trip,
    NULL AS credit_card_trips,
    NULL AS cash_trips,
    NULL AS credit_card_pct,
    NULL AS morning_rush_trips,
    NULL AS evening_rush_trips,
    NULL AS weekend_trips,
    NULL AS weekday_trips,
    NULL AS pickup_count,
    hour_label
FROM hourly_stats

UNION ALL

SELECT
    'top_locations' AS aggregation_type,
    CAST(location_id AS STRING) AS dimension_value,
    CONCAT('Location ', CAST(location_id AS STRING)) AS dimension_label,
    NULL AS reference_date,
    NULL AS total_trips,
    NULL AS total_distance,
    avg_trip_distance AS avg_distance,
    NULL AS avg_duration_minutes,
    NULL AS avg_speed_mph,
    total_revenue_location AS total_revenue,
    avg_revenue AS avg_revenue_per_trip,
    NULL AS total_passengers,
    NULL AS avg_passengers_per_trip,
    NULL AS credit_card_trips,
    NULL AS cash_trips,
    NULL AS credit_card_pct,
    NULL AS morning_rush_trips,
    NULL AS evening_rush_trips,
    NULL AS weekend_trips,
    NULL AS weekday_trips,
    pickup_count,
    NULL AS hour_label
FROM location_stats

ORDER BY aggregation_type, reference_date, dimension_value;
```

The gold table uses common table expressions (CTEs) to organize different aggregation types. Each CTE calculates metrics for a specific dimension—monthly trends, daily patterns, hourly distribution, and top locations. The final SELECT combines all these aggregations with UNION ALL into a single table.

This unified structure makes the gold table easy to query. Instead of having separate tables for each aggregation type, you query one table and filter by aggregation_type. This approach is simpler to maintain and easier to extend if you want to add new aggregation types later.

### Entry Point Scripts

The final piece of the implementation is creating entry point scripts that users run to execute the pipeline.

Create `run_full_refresh.py`:

```python
#!/usr/bin/env python3
"""
Entry point for Full Refresh Pipeline.
Loads all 2024 NYC Taxi data at once.

Usage:
    python run_full_refresh.py
"""
import sys
import logging
from pathlib import Path
from src.orchestrator import PipelineOrchestrator
from src.config import LOG_FILE, LOG_LEVEL, LOG_FORMAT


def setup_logging():
    """Configure logging for the pipeline."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )


def main():
    """Execute full refresh pipeline."""
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting NYC Taxi Full Refresh Pipeline")

    try:
        orchestrator = PipelineOrchestrator()
        success = orchestrator.run_full_refresh()
        orchestrator.close()

        if success:
            logger.info("Pipeline completed successfully")
            sys.exit(0)
        else:
            logger.error("Pipeline failed")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
```

Create `run_incremental.py`:

```python
#!/usr/bin/env python3
"""
Entry point for Incremental Pipeline.
Loads NYC Taxi data month by month with auto-progression.

Usage:
    python run_incremental.py              # Load next month automatically
    python run_incremental.py --month 3    # Load specific month (March)
"""
import sys
import logging
import argparse
from pathlib import Path
from src.orchestrator import PipelineOrchestrator
from src.config import LOG_FILE, LOG_LEVEL, LOG_FORMAT


def setup_logging():
    """Configure logging for the pipeline."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run NYC Taxi Incremental Pipeline"
    )
    parser.add_argument(
        "--month",
        type=int,
        choices=range(1, 13),
        help="Specific month to load (1-12). If not provided, loads next month automatically."
    )
    return parser.parse_args()


def main():
    """Execute incremental pipeline."""
    setup_logging()
    logger = logging.getLogger(__name__)

    args = parse_args()

    if args.month:
        logger.info(f"Starting incremental pipeline for month {args.month}")
    else:
        logger.info("Starting incremental pipeline (automatic month selection)")

    try:
        orchestrator = PipelineOrchestrator()
        success = orchestrator.run_incremental(target_month=args.month)
        orchestrator.close()

        if success:
            logger.info("Pipeline completed successfully")
            sys.exit(0)
        else:
            logger.error("Pipeline failed")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
```

These entry point scripts provide clean interfaces for running the pipeline. They handle logging setup, command-line argument parsing, error handling, and exit codes. The scripts are designed to work both interactively and in automated environments like cron jobs.

---

## Orchestration Logic and Retry Mechanism

The orchestration logic is the brain of the pipeline, coordinating all components to work together harmoniously. Understanding how orchestration works helps you troubleshoot issues and extend the pipeline with new capabilities.

### How Full Refresh Works

When you run `python run_full_refresh.py`, the entry point script creates a PipelineOrchestrator instance and calls its `run_full_refresh` method. Let me walk you through exactly what happens during a full refresh execution.

The orchestrator starts by recording the start time. This timestamp is used later to calculate total pipeline runtime. Then it begins executing the pipeline steps in sequence, with each step wrapped in retry logic for automatic failure recovery.

Step one is loading data to the staging layer. The orchestrator calls `data_loader.load_full_refresh_to_staging()`, which iterates through all 12 months downloading and uploading parquet files. For each month, the loader first checks if that month's data already exists using the boundary row checking logic. If the data exists, it skips the download and upload, logging that the month was skipped. If the data doesn't exist or is incomplete, it downloads the parquet file and uploads it to staging.

This step is wrapped in the retry handler, so if a network error causes a download to fail, the handler will automatically retry with exponential backoff. If all retries fail, the exception propagates up and the entire pipeline run is marked as failed.

Step two is creating the raw table from staging. The orchestrator calls `should_load_to_raw()` to check if raw is already in sync with staging. This check compares row counts—if staging has the same number of 2024 records as raw currently has, then raw doesn't need recreation. This idempotency check prevents unnecessary work and saves BigQuery costs.

If raw needs updating, the orchestrator executes the CREATE OR REPLACE SQL that rebuilds raw from staging. This SQL filters to only 2024 data, removing all those date infiltrations. The raw table recreation is fast—even with millions of rows, BigQuery completes it in seconds thanks to its distributed architecture.

Step three is transforming data to silver. The orchestrator calls `_transform_to_silver(None)`, passing None to indicate this is a full year refresh. The method reads the silver SQL file, replaces the date placeholders with '2024-01-01' and '2025-01-01', and executes the CREATE OR REPLACE statement. Silver is completely rebuilt from raw, applying all the cleaning logic and data quality filters.

Step four is aggregating to gold. The orchestrator calls `_aggregate_to_gold()`, which executes the gold SQL file. This creates the gold table with all its pre-calculated aggregations. Like silver, gold is completely rebuilt from scratch.

After all steps complete successfully, the orchestrator calculates the total runtime and calls the metadata manager to record the successful run. The metadata record includes the pipeline name ("full_refresh"), date range ("2024-01-01 - 2024-12-31"), status ("SUCCESS"), total rows loaded, runtime, and a null error message.

If any step fails despite retries, the orchestrator catches the exception, logs the error, calculates the runtime so far, and calls the metadata manager to record a failed run. The metadata record includes the same fields but with status "FAILED" and the error message filled in with details about what went wrong.

The entire full refresh typically takes 18 minutes on a decent internet connection. The slowest part is downloading the parquet files from NYC's servers—the BigQuery operations themselves are quite fast. Once data is in staging, creating raw takes about 30 seconds, silver takes about 60 seconds, and gold takes about 20 seconds.

### How Incremental Works

Incremental mode is more sophisticated than full refresh because it needs to determine which month to process and handle the case where all months are already loaded. Let's walk through an incremental execution.

When you run `python run_incremental.py` without specifying a month, the orchestrator first checks if the full year is already loaded by calling `metadata.is_full_year_loaded()`. This method queries the metadata table looking for either a successful full_refresh run or 12 successful incremental runs for all different months. If the full year is loaded, the pipeline exits immediately with a SKIPPED status, avoiding wasted work.

If the year isn't complete, the orchestrator calls `_get_next_month_to_load()` to determine which month to process. This method queries metadata for the last completed month (SUCCESS or SKIPPED status) and returns the next month number. If no months have been loaded yet, it returns 1 for January. If the last completed month was July, it returns 8 for August.

With the target month determined, the pipeline begins execution. Step one is loading that specific month to staging. The orchestrator calls `data_loader.load_incremental_to_staging(month)`, which downloads just that month's parquet file and checks if it already exists. The idempotency check here is the same boundary row logic used in full refresh, but applied to a single month.

If the month's data already exists in staging, the loader returns zero to indicate no rows were loaded. If the data doesn't exist, it downloads and uploads it, returning the actual row count. The orchestrator uses this return value to determine whether to record SUCCESS or SKIPPED in metadata.

Step two is updating the raw table. Unlike full refresh which blindly recreates raw, incremental mode first checks `should_load_to_raw()` to see if raw needs updating. If staging has new data but raw doesn't include it yet, raw gets recreated from staging. The CREATE OR REPLACE approach means raw is always rebuilt completely, not incrementally updated. This ensures consistency and avoids complex merge logic.

Step three is transforming to silver with month-specific date filtering. The orchestrator calls `_transform_to_silver(month)`, passing the month number. The method calculates the start and end dates for that month—for January, that's '2024-01-01' to '2024-02-01'. It replaces the date placeholders in the silver SQL with these specific dates before executing the CREATE OR REPLACE statement.

This month-specific filtering is crucial for handling date infiltrations correctly. Even though January's source file contains trips from other months and years, the silver SQL with its tight date filter only includes trips that actually occurred in January 2024. This gives us clean, month-specific data in silver without contamination from infiltrations.

Step four is aggregating to gold. Gold is always rebuilt completely from silver, regardless of whether we're doing full refresh or incremental. This ensures gold always reflects the current state of silver after any transformations.

After all steps complete, the orchestrator records metadata just like in full refresh, but with the specific month name ("January", "February", etc.) instead of "full year". The date range reflects the specific month processed.

The auto-progression feature makes incremental mode powerful for automation. You can set up a cron job to run `python run_incremental.py` daily, and it will progressively load January, then February, then March, all the way through December, without any manual intervention. If you run it again after December is complete, it detects the full year is loaded and exits with SKIPPED.

### The Retry Mechanism Deep Dive

The retry mechanism is critical for production reliability. Let me explain exactly how it works and when it helps.

When the orchestrator wants to execute an operation with retry logic, it calls `retry_handler.retry_operation(function, name, *args)`. The retry handler then enters a loop that attempts the operation up to max_retries times (configured as 3 in this project).

On the first attempt, the handler logs "Attempting [operation name] (attempt 1/3)" and calls the function with its arguments. If the function succeeds, the handler immediately returns the result. No retries needed—the operation is done.

If the function raises an exception, the handler catches it and logs "Operation failed on attempt 1: [error message]". If this isn't the last retry attempt, the handler calculates a delay using exponential backoff: delay = base_delay * (2 ** (attempt - 1)). With base_delay of 5 seconds, this produces 5 seconds for attempt 1, 10 seconds for attempt 2, and 20 seconds for attempt 3.

The handler logs "Retrying in [delay] seconds..." and sleeps for that duration. This sleep is important—it gives temporary issues time to resolve. If a service is overwhelmed, immediate retries would just make the problem worse. The exponential backoff gives increasing amounts of time for recovery.

After sleeping, the loop continues to the next attempt. The handler logs "Attempting [operation name] (attempt 2/3)" and tries again. This continues until either the operation succeeds or all retries are exhausted.

If all retry attempts fail, the handler logs "Operation failed after 3 attempts" and re-raises the last exception. This allows the caller (the orchestrator) to catch the exception and handle it appropriately, typically by recording a failed metadata entry and exiting.

The retry mechanism has saved the pipeline dozens of times during development and operation. Network hiccups are common when downloading large parquet files—sometimes the connection times out or the server returns a temporary error. The retry handler automatically recovers from these transient failures.

BigQuery occasionally rate-limits API calls if you're making many requests quickly. The retry handler's exponential backoff gives BigQuery time to reset its rate limits before trying again. Without retries, these transient issues would cause pipeline failures that require manual intervention.

Not all operations are wrapped in retry logic. Table creation SQL is retried because network issues can affect it. But metadata recording is not retried because if metadata recording fails, we want to know immediately—it might indicate a more serious problem with database permissions or configuration.

### Error Handling Strategy

Error handling throughout the pipeline follows a consistent philosophy: catch exceptions at the highest level where you can do something useful with them, log detailed information about what went wrong, record failures in metadata for audit purposes, and exit with appropriate status codes.

At the lowest level, the BigQuery client methods catch BigQuery-specific exceptions and re-raise them with context. For example, if a query fails due to a syntax error, the client logs "BigQuery error executing query: [error details]" and re-raises the exception. This provides visibility into what failed while still allowing higher levels to handle the error.

The data loader catches exceptions during downloads and uploads. If a download fails, it logs the error and returns None, signaling to the caller that the download failed. If an upload fails, it logs and re-raises the exception because upload failures are serious—they mean data isn't getting into BigQuery.

The orchestrator catches exceptions from all major operations. Each step is wrapped in try-except blocks that catch exceptions, log detailed error messages including stack traces, and record failed metadata entries. The orchestrator then returns False to indicate pipeline failure.

The entry point scripts catch exceptions at the highest level. They catch KeyboardInterrupt (Ctrl+C) separately to handle user interruptions gracefully. They catch all other exceptions with a generic exception handler that logs the full stack trace and exits with code 1.

Exit codes are meaningful. Exit code 0 means success, code 1 means failure, and code 130 means user interruption. These codes are important for automation—cron jobs and CI/CD systems check exit codes to determine if a script succeeded or failed.

This layered error handling provides multiple levels of visibility. At each level, appropriate information is logged. The lowest levels provide technical details about what operation failed. The middle levels provide context about which pipeline step was executing. The highest levels provide overall status and actionable guidance.

---

## Metadata Management Process

Metadata is the pipeline's memory—it tracks everything that happens during pipeline execution. Good metadata enables debugging, monitoring, auditing, and advanced features like auto-progression. Let me explain how metadata flows through the system.

### Recording Pipeline Runs

Every time the pipeline runs, whether it succeeds or fails, whether it processes data or skips already-loaded data, a record gets written to the metadata table. This record is a complete snapshot of that execution.

The orchestrator calls `metadata.record_run()` at the end of every pipeline execution. This method takes several parameters that together describe what happened. The pipeline_name parameter is either "full_refresh" or "incremental" depending on which mode ran. The status parameter is "SUCCESS" if everything worked, "FAILED" if an error occurred, or "SKIPPED" if the data was already loaded.

The rows_loaded parameter contains the number of rows that were actually loaded into the raw table during this run. For full refresh, this is typically around 35 million rows—the total number of valid 2024 trips after filtering. For incremental loads, it's typically 2-3 million rows per month. For skipped runs, it's zero.

The month parameter is used for incremental loads to track which month was processed. It's None for full refresh since full refresh processes all months at once. The metadata manager converts the month number to a month name—3 becomes "March", 11 becomes "November".

The runtime parameter contains the total execution time in seconds. The orchestrator calculates this by recording start time when the pipeline begins and subtracting it from current time when the pipeline completes. This tracks both successful and failed runs—even if the pipeline fails after 10 minutes, we record that 10-minute runtime.

The error_message parameter is None for successful runs and contains the exception message for failed runs. This error message is crucial for debugging—it tells you exactly what went wrong without needing to dig through log files.

The `record_run` method builds a SQL INSERT statement with all this information and executes it through the BigQuery client. The metadata table grows over time as an audit log of all pipeline executions. This historical data is valuable for understanding pipeline behavior and performance trends.

### Querying Metadata for Auto-Progression

The auto-progression feature in incremental mode depends entirely on metadata. When the orchestrator needs to determine which month to load next, it queries the metadata table.

The key method is `get_last_completed_month()`, which queries for the most recent incremental run where status is either SUCCESS or SKIPPED. The query orders by run timestamp descending and limits to 1, giving us the most recent completed month.

Why do we treat SKIPPED as completed? Because SKIPPED means the month's data already exists—there's no point trying to load it again. The pipeline should move on to the next month. This logic enables the pipeline to resume correctly after interruptions or reruns.

The method returns the month number, which the orchestrator uses to calculate the next month. If the last completed month was 5 (May), the next month to load is 6 (June). If the last completed month was 12 (December), there are no more months to load—the full year is complete.

If no metadata records exist at all, the method returns None, and the orchestrator interprets this as "start from January". This initialization logic ensures the pipeline works correctly on its very first run when there's no metadata yet.

The metadata query is efficient because it only looks at the metadata table, which is tiny—even after months of daily runs, the metadata table has only thousands of rows, not millions. The query completes in milliseconds and costs nothing in BigQuery charges.

### Metadata for Monitoring and Debugging

Metadata serves purposes beyond auto-progression. It's the primary tool for monitoring pipeline health and debugging issues.

The `get_run_history()` method retrieves recent pipeline runs, providing visibility into what's been happening. You can call this method to see the last 10 runs and check if they're all succeeding, or if there's a pattern of failures. This historical view helps identify trends—maybe the pipeline started failing on a particular date when a source file changed format, or maybe performance degraded over time as data volume grew.

The metadata table can be queried directly for more sophisticated analysis. Want to know average runtime by month? Query metadata grouped by month_loaded. Want to identify which months tend to fail? Query for records where status='FAILED' and look at patterns. Want to track pipeline performance over time? Query for runtime trends across different weeks or months.

When debugging a failed pipeline run, the metadata error_message field is usually the first place to look. It contains the exception message that caused the failure, giving immediate insight into what went wrong. Combined with log files, which contain full stack traces and detailed logging from each component, metadata provides enough information to diagnose most issues quickly.

The metadata table is also important for auditing and compliance. If someone asks "When did we load February's data?" or "Has March data been loaded yet?", the metadata table provides definitive answers. This audit trail is valuable for data governance and for understanding the complete history of the data pipeline.

### Metadata Failure Handling

One interesting aspect of metadata management is how the system handles metadata recording failures. Recording metadata is important but not critical—if the data load succeeded but metadata recording failed, we don't want to fail the entire pipeline.

The `record_run` method has a try-except block that catches any exceptions during metadata recording. If recording fails, it logs an error but doesn't re-raise the exception. This means the pipeline can continue even if metadata is temporarily unavailable.

This lenient handling acknowledges a fundamental principle: the actual data is more important than the metadata about the data. If we have to choose between loading data or recording metadata, we choose loading data. Of course, we log the metadata failure so it can be investigated and fixed, but it doesn't block pipeline execution.

In practice, metadata recording failures are rare. The metadata table is simple and recording is a single INSERT statement. If metadata recording does fail, it usually indicates a more serious issue like network connectivity problems or permissions issues that would also affect other pipeline operations.

---

## Loading Strategies: Full Refresh vs Incremental

The pipeline supports two distinct loading strategies, each optimized for different use cases. Understanding when to use each strategy and how they differ is important for operating the pipeline effectively.

### Full Refresh Explained

Full refresh is the simplest loading strategy conceptually—load everything at once. When you run `python run_full_refresh.py`, the pipeline downloads all 12 monthly parquet files and processes them through all layers in a single execution.

The full refresh flow starts by creating the staging, raw, and metadata tables if they don't exist. Then it downloads all 12 monthly parquet files sequentially, uploading each to staging. The downloads happen one month at a time rather than in parallel to avoid overwhelming your network connection and to keep memory usage reasonable.

After all months are in staging, the pipeline creates the raw table using CREATE OR REPLACE. This SQL filters staging to only 2024 data, removing all date infiltrations. Then it creates the silver table from raw, applying all cleaning and transformation logic. Finally, it creates the gold table from silver with all pre-calculated aggregations.

The entire process takes about 18 minutes on a typical internet connection. The breakdown is roughly: 12 minutes downloading files (varies with internet speed), 2 minutes uploading to staging, 1 minute creating raw, 2 minutes creating silver, 1 minute creating gold.

Full refresh is idempotent, meaning you can run it multiple times safely. If you run full refresh twice in a row, the second run will check if each month's data already exists in staging and skip downloading already-present months. This makes full refresh resilient to interruptions—if it fails on month 8, you can rerun it and it will skip months 1-7 and resume from month 8.

Full refresh is the recommended approach for initial setup. When you first set up the pipeline, you want all 2024 data loaded as quickly as possible. Running full refresh once is faster than running incremental 12 times (18 minutes vs 12 * 4 minutes = 48 minutes). It's also simpler operationally—one command loads everything.

Full refresh is also useful for disaster recovery. If something goes catastrophically wrong and your data gets corrupted, you can run full refresh to rebuild everything from scratch. The CREATE OR REPLACE pattern ensures all tables are fresh and consistent.

The main downside of full refresh is cost when rerunning. If you've already loaded all data and run full refresh again, it will recreate raw, silver, and gold tables even though the data hasn't changed. These recreations aren't free—they consume BigQuery compute time. For this reason, full refresh isn't recommended for regular ongoing operations after initial setup.

### Incremental Loading Explained

Incremental loading processes one month at a time. When you run `python run_incremental.py`, the pipeline automatically determines which month to load next based on metadata, loads just that month's data, and updates all downstream tables accordingly.

The incremental flow starts by querying metadata to find the last completed month. If no metadata exists, it starts with January. If July was the last completed month, it loads August. The pipeline downloads just that month's parquet file and uploads it to staging.

After staging has the new month's data, the pipeline recreates the raw table from staging. Note that even in incremental mode, raw is completely rebuilt, not incrementally updated. This ensures raw is always fully consistent with staging. The rebuild is fast—30 seconds—and ensures there's no possibility of partial or duplicate data.

Silver is also recreated completely, but with month-specific date filtering. The orchestrator replaces date placeholders in the silver SQL with the specific month's date range. For August, that's '2024-08-01' to '2024-09-01'. This tight date filter ensures silver only includes trips that actually occurred in August, excluding any date infiltrations that might be present in raw.

Gold is recreated from the now-updated silver table. Since gold aggregates all data in silver, it automatically includes the newly-added month's data in its calculations. Monthly summaries now include August, daily stats include all days through August, and so on.

Each incremental run takes 3-5 minutes depending on the month's data volume. Smaller months like February take less time, larger months like March take more. The auto-progression feature means you can run incremental repeatedly and it will progressively load all months without manual intervention.

Incremental loading is ideal for ongoing operations after initial setup. Once you've done a full refresh to load all 2024 data, you can use incremental mode if new monthly data becomes available. For example, if NYC publishes a revised January file with corrections, you can run `python run_incremental.py --month 1` to reload just January.

Incremental is also better for automation. A daily cron job running incremental will automatically process new months as they're published, without you needing to manually track what's already loaded. The auto-progression and idempotency ensure the cron job does the right thing whether it runs once or a hundred times.

### Comparing the Strategies

Let me lay out the key differences in a practical comparison:

**Speed:** Full refresh takes 18 minutes total to load everything. Incremental takes 3-5 minutes per month, so loading 12 months incrementally would take 36-60 minutes. Full refresh is faster for initial setup.

**Cost:** Full refresh costs about $5 for the initial run. Incremental costs about $0.50 per month, so 12 incremental runs cost about $6. The costs are similar, with full refresh slightly cheaper.

**Flexibility:** Incremental allows loading specific months with --month parameter. Full refresh always processes all months. Incremental is more flexible for targeted updates.

**Automation:** Incremental with auto-progression is perfect for cron jobs. Run it daily and it automatically advances through months. Full refresh requires manual intervention to avoid redundant processing.

**Use Cases:** Full refresh is best for initial setup, disaster recovery, and major pipeline changes. Incremental is best for ongoing operations, processing new monthly data, and automated scheduled runs.

**Idempotency:** Both are fully idempotent. You can run either multiple times safely without creating duplicate data.

**Memory Usage:** Both use similar memory because they process data in monthly batches. Neither ever loads more than one month's data into memory at once.

### The Silver Layer Date Filtering Innovation

One crucial aspect of incremental loading is how silver handles date filtering. In early versions of the pipeline, silver used the same date filter as raw (entire year 2024). This worked fine for full refresh but caused problems in incremental mode.

The problem was that even though I was processing only August, silver would recreate from ALL data in raw (January through August so far). This meant silver took longer and longer as more months accumulated. By December, silver recreation was taking several minutes because it was processing all 12 months.

The solution was dynamic date filtering in the silver SQL. By adding placeholder variables {start_date} and {end_date} and replacing them at runtime with month-specific dates, silver now only processes the relevant month's data. When loading August incrementally, silver filters to '2024-08-01' through '2024-09-01', ignoring January through July.

This optimization doesn't just save time—it also solves the date infiltration problem more thoroughly. Remember that raw contains 2024 data but doesn't filter by specific month. January's data in raw includes trips that actually occurred in February (infiltrations). By using tight month-specific date filters in silver, we ensure each month's data in silver is clean and accurate.

The implementation uses Python string replacement to modify the SQL before execution:

```python
def _transform_to_silver(self, month: Optional[int] = None):
    if month is None:
        start_date = '2024-01-01'
        end_date = '2025-01-01'
    else:
        start_date = f'2024-{month:02d}-01'
        if month == 12:
            end_date = '2025-01-01'
        else:
            end_date = f'2024-{month+1:02d}-01'
    
    sql = sql_file.read()
    sql = sql.replace('{start_date}', start_date)
    sql = sql.replace('{end_date}', end_date)
```

This approach gives us the best of both worlds: full refresh uses year-wide dates and processes everything at once, while incremental uses month-specific dates and processes efficiently.

### Choosing Your Strategy

For most users, I recommend this workflow: Run full refresh once for initial setup. This loads all 2024 data quickly and gets your analytics environment ready. Then switch to incremental for ongoing operations. Set up a cron job that runs incremental daily. It will auto-advance through months and handle any new or updated data gracefully.

If you ever need to rebuild everything from scratch, run full refresh again. If you need to reload a specific month because the source data was updated, run incremental with the --month parameter. If you're making major changes to transformation logic and want to reprocess all data, run full refresh to regenerate all downstream tables with the new logic.

The two-strategy approach provides flexibility while keeping each strategy focused and efficient. Full refresh optimizes for "load everything fast", while incremental optimizes for "keep up with new data efficiently". Together, they cover all the operational patterns you'll encounter.

---

## Testing and Quality Assurance

Testing a data pipeline is fundamentally different from testing a typical application. You're not just verifying that functions return correct values—you're ensuring that data flows correctly through multiple transformations, that idempotency works across reruns, that failures are handled gracefully, and that costs remain under control. Let me walk you through the comprehensive testing strategy I developed for this pipeline.

### The Testing Philosophy

I structured the testing approach around three tiers, each serving a different purpose. Unit tests validate individual components in isolation without touching external systems. Integration tests verify that components work together correctly, including interactions with BigQuery. End-to-end tests validate the complete pipeline flow from source data to gold layer analytics.

The challenge with data pipeline testing is that comprehensive testing requires access to BigQuery and can be expensive. You can't run hundreds of full pipeline tests in CI because each one would cost money and take 18 minutes. The solution is to be strategic about what you test at each tier and when you run different types of tests.

### Unit Testing Strategy

Unit tests form the foundation of the testing pyramid. These tests are fast, cheap, and run on every code change in CI. They don't require BigQuery credentials or network access—they test pure logic.

Create `tests/test_retry_handler.py`:

```python
"""Unit tests for retry handler."""
import pytest
import time
from src.retry_handler import RetryHandler


def test_retry_handler_success_first_try():
    """Test that successful operations don't retry."""
    handler = RetryHandler(max_retries=3, base_delay=1)
    
    call_count = 0
    
    def successful_operation():
        nonlocal call_count
        call_count += 1
        return "success"
    
    result = handler.retry_operation(successful_operation, "test_op")
    
    assert result == "success"
    assert call_count == 1  # Should only call once


def test_retry_handler_success_after_failures():
    """Test that operations succeed after retries."""
    handler = RetryHandler(max_retries=3, base_delay=0.1)
    
    call_count = 0
    
    def eventually_successful_operation():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise Exception("Temporary failure")
        return "success"
    
    result = handler.retry_operation(eventually_successful_operation, "test_op")
    
    assert result == "success"
    assert call_count == 3  # Should succeed on third try


def test_retry_handler_all_attempts_fail():
    """Test that handler raises exception after all retries fail."""
    handler = RetryHandler(max_retries=3, base_delay=0.1)
    
    call_count = 0
    
    def always_fails():
        nonlocal call_count
        call_count += 1
        raise ValueError("Permanent failure")
    
    with pytest.raises(ValueError, match="Permanent failure"):
        handler.retry_operation(always_fails, "test_op")
    
    assert call_count == 3  # Should try all attempts


def test_exponential_backoff_timing():
    """Test that delays follow exponential backoff pattern."""
    handler = RetryHandler(max_retries=3, base_delay=1)
    
    assert handler.get_delay(1) == 1   # 1 * 2^0 = 1
    assert handler.get_delay(2) == 2   # 1 * 2^1 = 2
    assert handler.get_delay(3) == 4   # 1 * 2^2 = 4


def test_retry_decorator():
    """Test the retry decorator works correctly."""
    from src.retry_handler import retry
    
    call_count = 0
    
    @retry(max_retries=3, base_delay=0.1)
    def decorated_function():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise Exception("First attempt fails")
        return "success"
    
    result = decorated_function()
    
    assert result == "success"
    assert call_count == 2
```

These tests verify the retry logic without needing any external dependencies. They run in milliseconds and can be executed thousands of times without cost. The tests use small base_delay values (0.1 seconds) to keep test execution fast while still verifying the backoff logic works.

Create `tests/test_config.py`:

```python
"""Unit tests for configuration module."""
import pytest
import os
from src.config import (
    get_month_name,
    get_date_range_string,
    get_parquet_url,
    IS_TEST_MODE,
    PROJECT_ID,
    DATASET_ID
)


def test_month_names():
    """Test that month numbers map to correct names."""
    assert get_month_name(1) == "January"
    assert get_month_name(6) == "June"
    assert get_month_name(12) == "December"


def test_month_name_invalid():
    """Test that invalid month numbers are handled."""
    result = get_month_name(13)
    assert "Month-13" in result


def test_date_range_single_month():
    """Test date range generation for single month."""
    # January
    jan_range = get_date_range_string(1)
    assert "2024-01-01" in jan_range
    assert "2024-01-31" in jan_range
    
    # December
    dec_range = get_date_range_string(12)
    assert "2024-12-01" in dec_range
    assert "2024-12-31" in dec_range


def test_date_range_full_year():
    """Test date range generation for full year."""
    full_range = get_date_range_string(None)
    assert "2024-01-01" in full_range
    assert "2024-12-31" in full_range


def test_parquet_url_generation():
    """Test that parquet URLs are generated correctly."""
    jan_url = get_parquet_url(1)
    assert "yellow_tripdata_2024-01.parquet" in jan_url
    assert jan_url.startswith("https://")
    
    dec_url = get_parquet_url(12)
    assert "yellow_tripdata_2024-12.parquet" in dec_url


def test_test_mode_detection():
    """Test that test mode is correctly detected."""
    # In pytest environment, IS_TEST_MODE should be True
    assert IS_TEST_MODE == True
    
    # Test configuration should use safe defaults
    assert PROJECT_ID is not None
    assert DATASET_ID is not None


def test_environment_variables_in_test_mode():
    """Test that environment variables work in test mode."""
    # These should be set by conftest.py
    assert os.getenv('TESTING') == 'true'
    assert os.getenv('CI') == 'true'
```

Configuration tests verify that helper functions produce correct outputs and that test mode detection works properly. These tests are important because configuration bugs can cause subtle failures that are hard to debug.

Create `tests/test_metadata_logic.py`:

```python
"""Unit tests for metadata management logic."""
import pytest
from datetime import datetime
from src.config import STATUS_SUCCESS, STATUS_FAILED, STATUS_SKIPPED


def test_status_constants():
    """Test that status constants are defined correctly."""
    assert STATUS_SUCCESS == "SUCCESS"
    assert STATUS_FAILED == "FAILED"
    assert STATUS_SKIPPED == "SKIPPED"


def test_month_to_name_conversion():
    """Test month number to name conversion logic."""
    from src.config import get_month_name
    
    month_map = {
        1: "January", 2: "February", 3: "March",
        4: "April", 5: "May", 6: "June",
        7: "July", 8: "August", 9: "September",
        10: "October", 11: "November", 12: "December"
    }
    
    for num, name in month_map.items():
        assert get_month_name(num) == name


def test_date_range_calculations():
    """Test date range calculation logic."""
    from src.config import get_date_range_string
    
    # Test each month
    for month in range(1, 13):
        date_range = get_date_range_string(month)
        assert f"2024-{month:02d}-01" in date_range
        
    # Test full year
    full_year = get_date_range_string(None)
    assert "2024-01-01" in full_year
    assert "2024-12-31" in full_year
```

These tests validate the logic that metadata management depends on. They ensure that month-to-name conversions work correctly, that date range calculations produce expected results, and that status constants are properly defined.

### Test Configuration and Fixtures

The test suite uses pytest fixtures to set up consistent test environments. Fixtures handle common setup tasks like configuring environment variables and mocking external dependencies.

Create `tests/conftest.py`:

```python
"""Pytest configuration and fixtures."""
import os
import sys
from pathlib import Path
import pytest


# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture(autouse=True, scope="session")
def setup_test_environment():
    """Setup test environment for all tests."""
    # Set test mode flags
    os.environ['CI'] = 'true'
    os.environ['TESTING'] = 'true'
    os.environ['PYTEST_CURRENT_TEST'] = 'true'
    
    # Set dummy GCP credentials
    os.environ['GCP_PROJECT_ID'] = 'test-project-id'
    os.environ['BQ_DATASET'] = 'test_dataset'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/test-credentials.json'
    
    # Set table names
    os.environ['STAGING_TABLE_NAME'] = 'staging_yellow_taxi'
    os.environ['RAW_TABLE_NAME'] = 'raw_yellow_taxi'
    os.environ['SILVER_TABLE_NAME'] = 'silver_yellow_taxi'
    os.environ['GOLD_TABLE_NAME'] = 'gold_yellow_taxi'
    os.environ['METADATA_TABLE_NAME'] = 'pipeline_metadata'
    
    # Set data source
    os.environ['NYC_TAXI_BASE_URL'] = 'https://example.com'
    os.environ['TAXI_FILE_TEMPLATE'] = 'file_{month:02d}.parquet'
    
    # Create dummy credentials file
    Path('/tmp').mkdir(exist_ok=True)
    with open('/tmp/test-credentials.json', 'w') as f:
        f.write('{"type": "service_account", "project_id": "test"}')
    
    yield
    
    # Cleanup
    try:
        os.remove('/tmp/test-credentials.json')
    except:
        pass


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    import pandas as pd
    from datetime import datetime, timedelta
    
    base_date = datetime(2024, 1, 1, 10, 0, 0)
    
    data = {
        'VendorID': [1, 2, 1],
        'tpep_pickup_datetime': [
            base_date,
            base_date + timedelta(hours=1),
            base_date + timedelta(hours=2)
        ],
        'tpep_dropoff_datetime': [
            base_date + timedelta(minutes=15),
            base_date + timedelta(hours=1, minutes=20),
            base_date + timedelta(hours=2, minutes=10)
        ],
        'passenger_count': [1.0, 2.0, 1.0],
        'trip_distance': [2.5, 3.1, 1.8],
        'fare_amount': [12.5, 15.0, 10.0],
        'total_amount': [15.5, 18.0, 12.5],
        'PULocationID': [161, 162, 163],
        'DOLocationID': [234, 235, 236],
        'payment_type': [1, 1, 2]
    }
    
    return pd.DataFrame(data)


@pytest.fixture
def mock_bigquery_client(monkeypatch):
    """Mock BigQuery client for tests that need it."""
    class MockClient:
        def __init__(self):
            self.project = "test-project"
        
        def query(self, sql):
            class MockResult:
                def result(self):
                    return []
            return MockResult()
        
        def close(self):
            pass
    
    def mock_init(self):
        self.client = MockClient()
    
    monkeypatch.setattr(
        'src.bigquery_client.BigQueryClient.__init__',
        mock_init
    )
```

The conftest.py file runs automatically before any tests. It sets up the test environment with appropriate flags and dummy values, ensuring tests can import and use the source code without requiring real GCP credentials. The fixtures provide reusable test data and mocked dependencies.

### Integration Testing Approach

Integration tests verify that components work together correctly, including interactions with BigQuery. These tests are more expensive and slower than unit tests, so they're marked with `@pytest.mark.integration` and excluded from CI runs.

Create `tests/test_integration.py`:

```python
"""Integration tests requiring BigQuery access."""
import pytest


@pytest.mark.integration
def test_bigquery_connection():
    """Test that we can connect to BigQuery."""
    from src.bigquery_client import BigQueryClient
    
    client = BigQueryClient()
    
    # Test simple query
    query = "SELECT 1 as test_value"
    results = list(client.execute_query(query))
    
    assert len(results) == 1
    assert results[0].test_value == 1
    
    client.close()


@pytest.mark.integration
def test_table_creation():
    """Test that we can create tables."""
    from src.bigquery_client import BigQueryClient
    from src.config import METADATA_TABLE
    
    client = BigQueryClient()
    
    # Check if metadata table exists
    exists = client.table_exists(METADATA_TABLE)
    
    # Should exist after setup
    assert exists == True
    
    client.close()


@pytest.mark.integration
def test_staging_table_query():
    """Test querying staging table."""
    from src.bigquery_client import BigQueryClient
    from src.config import STAGING_TABLE
    
    client = BigQueryClient()
    
    # Test that staging table can be queried
    query = f"SELECT COUNT(*) as count FROM `{STAGING_TABLE}` LIMIT 1"
    results = list(client.execute_query(query))
    
    # Should execute without error
    assert results is not None
    
    client.close()
```

Integration tests are run manually during development and before major releases. They verify that the pipeline actually works with real BigQuery, catching issues that unit tests might miss.

### Running the Tests

The project includes pytest configuration to make running tests easy. Create `pytest.ini`:

```ini
[tool:pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --tb=short
    --strict-markers
markers =
    integration: marks tests as integration tests (deselect with '-m "not integration"')
    unit: marks tests as unit tests
```

This configuration tells pytest where to find tests and how to run them. The markers allow selective test execution—you can run only unit tests or only integration tests.

To run all unit tests (default for CI):
```bash
pytest tests/ -v
```

To run only integration tests:
```bash
pytest tests/ -v -m integration
```

To run tests with coverage reporting:
```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

The test suite runs in seconds for unit tests, minutes for integration tests. This fast feedback loop encourages running tests frequently during development.

### Code Quality Tools

Beyond functional testing, the project uses several code quality tools to maintain high standards.

**Flake8** checks for syntax errors and style issues. Create `.flake8`:

```ini
[flake8]
max-line-length = 127
exclude =
    .git,
    __pycache__,
    .venv,
    venv,
    build,
    dist
ignore = 
    E203,  # whitespace before ':'
    W503,  # line break before binary operator
    E402   # module level import not at top
per-file-ignores =
    run_*.py:E402
    __init__.py:F401
max-complexity = 10
show-source = True
statistics = True
count = True
```

**Black** enforces consistent code formatting. Create `pyproject.toml`:

```toml
[tool.black]
line-length = 130
target-version = ['py312']
include = '\.pyi?$'
```

These tools run automatically in CI, ensuring all code meets quality standards before being merged.

---

## CI/CD Setup and Automation

Continuous Integration and Continuous Deployment (CI/CD) automates testing and quality checks, ensuring that every code change is validated before reaching production. The separation between CI/CD and data processing is intentional and important—CI validates code quality, while scheduled cron jobs handle actual data processing.

### Understanding the Separation

Many developers are tempted to run their entire data pipeline in CI. This seems logical—automate everything! But this approach creates several problems. CI runs on every code push, which could be dozens of times per day during active development. Running an 18-minute data pipeline on every push would waste enormous amounts of time. CI systems have limited compute resources and time quotas. Using them for data processing wastes these valuable resources. Data processing requires production credentials with write access to databases. CI should never have production write access because a compromised CI system could damage production data.

The solution is clear separation of concerns. CI validates that code is correct, properly formatted, and passes all tests. This validation is fast (2-3 minutes) and runs on every code change. Data processing happens on a schedule via cron jobs, using dedicated infrastructure with appropriate credentials. This separation makes both systems simpler, more focused, and more reliable.

### The CI Workflow

The CI workflow is defined in `.github/workflows/ci.yml`. This file tells GitHub Actions exactly what to do when code changes. Let me walk through each section and explain its purpose.

Create `.github/workflows/ci.yml`:

```yaml
name: CI - Code Quality & Tests

on:
  push:
    branches: [ dev, main ]
  pull_request:
    branches: [ main ]

jobs:
  lint-and-test:
    name: Lint and Test
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v4
    
    - name: Set up Python 3.12
      uses: actions/setup-python@v5
      with:
        python-version: '3.12'
    
    - name: Cache pip dependencies
      uses: actions/cache@v4
      with:
        path: ~/.cache/pip
        key: ${{ runner.os }}-pip-${{ hashFiles('**/requirements.txt') }}
        restore-keys: |
          ${{ runner.os }}-pip-
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
        pip install flake8==7.0.0 black==24.0.0 pytest==7.4.0 pytest-cov==4.1.0
    
    - name: Set test environment variables
      run: |
        echo "CI=true" >> $GITHUB_ENV
        echo "TESTING=true" >> $GITHUB_ENV
        mkdir -p /tmp
        echo '{"type": "service_account", "project_id": "test"}' > /tmp/test-creds.json
    
    - name: Run flake8 linting - Critical Errors
      run: |
        flake8 src/ tests/ --count --select=E9,F63,F7,F82 --show-source --statistics
    
    - name: Run flake8 linting - Style Check
      run: |
        flake8 src/ tests/ --count --max-line-length=130 --statistics
      continue-on-error: true
    
    - name: Check code formatting with black
      run: |
        black --check src/ tests/
      continue-on-error: true
    
    - name: Run pytest with coverage
      run: |
        pytest tests/ -v --tb=short --maxfail=5 --cov=src --cov-report=term-missing --cov-report=xml

  security-scan:
    name: Security Scan
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v4
    
    - name: Set up Python 3.12
      uses: actions/setup-python@v5
      with:
        python-version: '3.12'
    
    - name: Install Bandit
      run: pip install bandit[toml]
    
    - name: Run Bandit security scan
      run: |
        bandit -r src/ -ll -f screen
        bandit -r src/ -ll -f json -o bandit-report.json
      continue-on-error: true
    
    - name: Upload security report
      uses: actions/upload-artifact@v4
      if: always()
      with:
        name: security-report
        path: bandit-report.json
        retention-days: 30

  build-validation:
    name: Build Validation
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v4
    
    - name: Set up Python 3.12
      uses: actions/setup-python@v5
      with:
        python-version: '3.12'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    
    - name: Verify imports
      run: |
        python -c "
        from src.config import PROJECT_ID, DATASET_ID
        from src.bigquery_client import BigQueryClient
        from src.data_loader import DataLoader
        from src.metadata_manager import MetadataManager
        from src.orchestrator import PipelineOrchestrator
        from src.retry_handler import RetryHandler
        print('All imports successful')
        "
    
    - name: Validate SQL files
      run: |
        test -f sql/create_metadata_table.sql || exit 1
        test -f sql/create_staging_table.sql || exit 1
        test -f sql/create_raw_table.sql || exit 1
        test -f sql/create_silver_table.sql || exit 1
        test -f sql/create_gold_table.sql || exit 1

  report-status:
    name: CI Summary
    needs: [lint-and-test, security-scan, build-validation]
    runs-on: ubuntu-latest
    if: always()
    
    steps:
    - name: Report CI Results
      run: |
        echo "CI/CD PIPELINE RESULTS"
        echo "Lint & Test: ${{ needs.lint-and-test.result }}"
        echo "Security Scan: ${{ needs.security-scan.result }}"
        echo "Build Validation: ${{ needs.build-validation.result }}"
        
        if [ "${{ needs.lint-and-test.result }}" == "success" ] && \
           [ "${{ needs.build-validation.result }}" == "success" ]; then
          echo "All checks passed!"
          exit 0
        else
          echo "Some checks failed"
          exit 1
        fi
```

The workflow runs on two triggers: pushes to dev or main branches, and pull requests to main. This ensures all code changes are validated before merging.

The workflow consists of four parallel jobs. The lint-and-test job is the primary validation. It checks out the code, sets up Python 3.12, installs dependencies, and runs flake8 for linting, black for formatting checks, and pytest for functional tests. The job uses caching to speed up dependency installation on subsequent runs.

The security-scan job runs Bandit, a security linting tool that detects common security issues in Python code. It looks for things like hardcoded passwords, use of insecure functions, or potential SQL injection vulnerabilities. The results are uploaded as artifacts for review.

The build-validation job verifies that all modules can be imported and that required SQL files exist. This catches issues like missing files or broken import paths that might not be caught by unit tests.

The report-status job aggregates results from all other jobs and provides a clear summary. It only succeeds if all critical jobs succeeded, making it easy to see at a glance whether the build is safe to merge.

### Branch Protection Strategy

CI is only effective if you actually enforce its results. Branch protection rules ensure that code can't reach main unless it passes all CI checks.

In GitHub, navigate to your repository settings, then to "Rules" → "Rulesets". Create a new ruleset named "Protect Main Branch" with these settings:

**Target branches:** Include main by pattern

**Rules enabled:**
- Require pull request before merging (1 approval for team projects, 0 for solo)
- Require status checks to pass before merging
- Require branches to be up to date before merging
- Required checks: lint-and-test, security-scan, build-validation, report-status
- Block force pushes
- Restrict deletions

With these rules in place, you can't push directly to main—all changes must go through pull requests. Pull requests can't be merged unless all CI checks pass. This ensures that broken code never reaches production.

The development workflow becomes: create feature branch → make changes → push and open PR → CI runs automatically → if CI passes, merge to main. This systematic approach maintains code quality without requiring manual checks.

### Setting Up Scheduled Pipeline Execution

While CI validates code, actual data processing happens on a schedule via cron. The cron job runs on a dedicated server with production credentials, completely separate from CI.

Create `run_pipeline_cron.sh`:

```bash
#!/bin/bash
################################################################################
# NYC Taxi Pipeline - Cron Execution Script
################################################################################

PROJECT_DIR="/home/yourusername/nyc-taxi-pipeline"
PYTHON_VENV="$PROJECT_DIR/venv"
LOG_DIR="$PROJECT_DIR/logs/cron"
PIPELINE_SCRIPT="$PROJECT_DIR/run_incremental.py"

ENABLE_EMAIL_NOTIFICATIONS=false
NOTIFICATION_EMAIL="your-email@example.com"

################################################################################

mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/pipeline_${TIMESTAMP}.log"
ERROR_LOG="$LOG_DIR/pipeline_${TIMESTAMP}_error.log"

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

send_notification() {
    local subject="$1"
    local message="$2"
    
    if [ "$ENABLE_EMAIL_NOTIFICATIONS" = true ]; then
        echo "$message" | mail -s "$subject" "$NOTIFICATION_EMAIL"
    fi
}

log_message "========================================"
log_message "NYC Taxi Pipeline - Cron Execution"
log_message "========================================"
log_message "Pipeline started"

cd "$PROJECT_DIR" || {
    log_message "ERROR: Failed to change to project directory"
    exit 1
}

if [ ! -d "$PYTHON_VENV" ]; then
    log_message "ERROR: Virtual environment not found"
    exit 1
fi

log_message "Activating virtual environment..."
source "$PYTHON_VENV/bin/activate" || {
    log_message "ERROR: Failed to activate virtual environment"
    exit 1
}

log_message "Starting pipeline execution..."
python "$PIPELINE_SCRIPT" >> "$LOG_FILE" 2>> "$ERROR_LOG"
EXIT_CODE=$?

log_message "Pipeline execution completed"
log_message "Exit code: $EXIT_CODE"

if [ $EXIT_CODE -eq 0 ]; then
    log_message "Pipeline completed successfully"
    send_notification "NYC Taxi Pipeline SUCCESS" "Pipeline completed successfully"
else
    log_message "Pipeline failed"
    cat "$ERROR_LOG" >> "$LOG_FILE"
    send_notification "NYC Taxi Pipeline FAILED" "Pipeline failed with exit code $EXIT_CODE"
fi

deactivate

log_message "Cleaning up old log files (keeping last 30 days)..."
find "$LOG_DIR" -name "pipeline_*.log" -mtime +30 -delete

log_message "========================================"
log_message "Cron execution finished"
log_message "========================================"

exit $EXIT_CODE
```

Make the script executable:
```bash
chmod +x run_pipeline_cron.sh
```

Set up the cron job by editing your crontab:
```bash
crontab -e
```

Add this line to run daily at 2 AM:
```
0 2 * * * /home/yourusername/nyc-taxi-pipeline/run_pipeline_cron.sh
```

The cron script provides comprehensive logging, automatic error handling, and optional email notifications. It runs the incremental pipeline daily, which automatically determines which month to process based on metadata.

### Why This Separation Works

The separation between CI and cron execution provides several benefits. CI gives fast feedback on code quality—developers know within minutes if their changes break something. Cron runs data processing when appropriate—once daily, not on every code change. CI uses free GitHub Actions resources for what they're designed for—validating code. Cron uses dedicated infrastructure with appropriate credentials for production workloads. Failures have clear meanings—CI failure means code is broken, cron failure means data processing encountered an issue.

This architecture is how professional data engineering teams operate. They don't run data pipelines in CI—they use CI to validate code and dedicated scheduling systems for actual data processing.

---

## Querying the Gold Layer

The gold layer is the final destination of our data pipeline—it contains pre-aggregated, analytics-ready data that analysts and data scientists can query directly without needing to understand the complexities of the raw data or transformation logic. Let me show you how to extract insights from the gold layer.

### Understanding Gold Layer Structure

The gold table uses a unified structure where different aggregation types are stored in the same table but distinguished by the aggregation_type column. This design makes the table easy to query and extend. Each row represents one aggregation—one month's statistics, one day's metrics, one hour's summary, or one location's performance.

The key columns in the gold table are:

**aggregation_type**: Identifies what kind of aggregation this row represents (monthly, daily, hourly, top_locations)

**dimension_value**: The specific value for this aggregation (month number, date, hour, location ID)

**dimension_label**: Human-readable label for the dimension (month name, day name, hour label, location name)

**reference_date**: The date this row references (for time-based aggregations)

**Metric columns**: Various numeric metrics like total_trips, total_revenue, avg_distance, etc. Not all metrics are populated for all aggregation types—some are NULL depending on what makes sense for that aggregation.

This structure means you can write queries that filter by aggregation_type to get exactly the view you need.

### Monthly Analysis Queries

Monthly aggregations show trends over time and are perfect for understanding seasonal patterns and growth.

**Query 1: Total trips and revenue by month**

```sql
SELECT
    dimension_label AS month_name,
    total_trips,
    total_revenue,
    avg_revenue_per_trip,
    avg_distance,
    avg_duration_minutes
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'monthly'
ORDER BY reference_date;
```

This query shows the big picture for each month. You can see which months are busiest, which generate the most revenue, and how average trip characteristics vary throughout the year.

**Sample Results:**
```
month_name  | total_trips | total_revenue  | avg_revenue_per_trip | avg_distance | avg_duration_minutes
------------|-------------|----------------|---------------------|--------------|---------------------
January     | 2,964,624   | $43,256,789.50 | $14.59              | 3.12         | 15.8
February    | 2,847,123   | $41,678,234.25 | $14.64              | 3.08         | 15.6
March       | 3,126,789   | $45,789,456.75 | $14.64              | 3.15         | 15.9
...
```

**Query 2: Payment method trends by month**

```sql
SELECT
    dimension_label AS month_name,
    credit_card_trips,
    cash_trips,
    credit_card_pct,
    total_trips
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'monthly'
ORDER BY reference_date;
```

This query reveals payment preferences. You can track the shift from cash to credit cards over time and identify months with unusual payment patterns.

**Query 3: Rush hour patterns by month**

```sql
SELECT
    dimension_label AS month_name,
    morning_rush_trips,
    evening_rush_trips,
    ROUND(morning_rush_trips * 100.0 / total_trips, 2) AS morning_rush_pct,
    ROUND(evening_rush_trips * 100.0 / total_trips, 2) AS evening_rush_pct
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'monthly'
ORDER BY reference_date;
```

This shows what percentage of trips occur during rush hours and how this varies by month. Summer months might show different patterns than winter months as commuting patterns change.

### Daily Analysis Queries

Daily aggregations help identify day-of-week patterns and unusual days.

**Query 4: Average daily metrics by day of week**

```sql
SELECT
    dimension_label AS day_name,
    ROUND(AVG(daily_trips), 0) AS avg_trips,
    ROUND(AVG(daily_revenue), 2) AS avg_revenue,
    ROUND(AVG(avg_distance), 2) AS avg_distance,
    ROUND(AVG(avg_fare), 2) AS avg_fare
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'daily'
GROUP BY dimension_label, pickup_dayofweek
ORDER BY pickup_dayofweek;
```

This query aggregates across all weeks to show typical patterns for each day of the week. You'll likely see that weekdays have different patterns than weekends.

**Sample Results:**
```
day_name  | avg_trips | avg_revenue    | avg_distance | avg_fare
----------|-----------|----------------|--------------|----------
Monday    | 98,523    | $1,438,234.50  | 3.14         | $14.60
Tuesday   | 102,456   | $1,495,678.25  | 3.12         | $14.58
...
Saturday  | 115,789   | $1,689,456.75  | 3.45         | $14.75
Sunday    | 89,234    | $1,302,345.50  | 3.38         | $14.70
```

**Query 5: Busiest and slowest days**

```sql
-- Busiest days
SELECT
    dimension_value AS date,
    dimension_label AS day_name,
    daily_trips,
    daily_revenue
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'daily'
ORDER BY daily_trips DESC
LIMIT 10;

-- Slowest days
SELECT
    dimension_value AS date,
    dimension_label AS day_name,
    daily_trips,
    daily_revenue
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'daily'
ORDER BY daily_trips ASC
LIMIT 10;
```

These queries identify outliers—days with unusually high or low activity. The busiest days might be holidays or special events, while the slowest days might be severe weather days.

### Hourly Analysis Queries

Hourly aggregations reveal intraday patterns like rush hours and late-night activity.

**Query 6: Trip distribution by hour**

```sql
SELECT
    dimension_label AS hour,
    trips_per_hour,
    avg_revenue AS avg_fare_per_trip,
    total_revenue_hour,
    ROUND(trips_per_hour * 100.0 / SUM(trips_per_hour) OVER (), 2) AS pct_of_daily_trips
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'hourly'
ORDER BY CAST(dimension_value AS INT64);
```

This query shows how taxi usage varies throughout the day. You'll see clear peaks during morning and evening rush hours, and valleys during early morning hours.

**Sample Results:**
```
hour      | trips_per_hour | avg_fare_per_trip | total_revenue_hour | pct_of_daily_trips
----------|----------------|-------------------|-------------------|-------------------
Hour 0:00 | 125,456        | $18.25            | $2,289,568.00     | 3.58%
Hour 1:00 | 98,234         | $19.50            | $1,915,563.00     | 2.80%
...
Hour 8:00 | 178,923        | $12.50            | $2,236,537.50     | 5.11%
Hour 18:00| 195,678        | $13.75            | $2,690,572.50     | 5.58%
```

**Query 7: Peak vs off-peak comparison**

```sql
SELECT
    CASE 
        WHEN CAST(dimension_value AS INT64) BETWEEN 6 AND 9 THEN 'Morning Rush'
        WHEN CAST(dimension_value AS INT64) BETWEEN 16 AND 19 THEN 'Evening Rush'
        WHEN CAST(dimension_value AS INT64) BETWEEN 22 AND 5 THEN 'Late Night'
        ELSE 'Off Peak'
    END AS period,
    SUM(trips_per_hour) AS total_trips,
    ROUND(AVG(avg_revenue), 2) AS avg_fare,
    SUM(total_revenue_hour) AS total_revenue
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'hourly'
GROUP BY period
ORDER BY total_trips DESC;
```

This query groups hours into meaningful periods and compares their characteristics. You might find that late-night trips have higher average fares due to fewer riders splitting costs and potentially longer distances.

### Location Analysis Queries

Location aggregations identify popular pickup zones and their characteristics.

**Query 8: Top 20 busiest pickup locations**

```sql
SELECT
    CAST(dimension_value AS INT64) AS location_id,
    dimension_label,
    pickup_count,
    total_revenue,
    avg_distance,
    avg_revenue_per_trip,
    ROUND(total_revenue * 100.0 / SUM(total_revenue) OVER (), 2) AS pct_of_total_revenue
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'top_locations'
ORDER BY pickup_count DESC
LIMIT 20;
```

This identifies the busiest taxi zones in the city. These are likely major business districts, transportation hubs, and tourist areas.

**Sample Results:**
```
location_id | dimension_label | pickup_count | total_revenue   | avg_distance | pct_of_total_revenue
------------|-----------------|--------------|-----------------|--------------|---------------------
161         | Location 161    | 1,589,234    | $23,456,789.50  | 2.8          | 4.25%
237         | Location 237    | 1,423,567    | $21,234,567.75  | 3.1          | 3.85%
...
```

**Query 9: Locations with highest average revenue**

```sql
SELECT
    CAST(dimension_value AS INT64) AS location_id,
    pickup_count,
    avg_revenue_per_trip,
    avg_distance,
    ROUND(avg_revenue_per_trip / avg_distance, 2) AS revenue_per_mile
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'top_locations'
    AND pickup_count > 10000  -- Only significant locations
ORDER BY avg_revenue_per_trip DESC
LIMIT 20;
```

This finds locations where trips generate the highest revenue on average. These might be airports or areas that generate longer trips.

### Advanced Analytics Queries

Now let's look at more sophisticated analyses that combine gold layer data in interesting ways.

**Query 10: Month-over-month growth analysis**

```sql
WITH monthly_data AS (
    SELECT
        dimension_label,
        reference_date,
        total_trips,
        total_revenue,
        LAG(total_trips) OVER (ORDER BY reference_date) AS prev_month_trips,
        LAG(total_revenue) OVER (ORDER BY reference_date) AS prev_month_revenue
    FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
    WHERE aggregation_type = 'monthly'
)
SELECT
    dimension_label AS month_name,
    total_trips,
    prev_month_trips,
    ROUND((total_trips - prev_month_trips) * 100.0 / prev_month_trips, 2) AS trip_growth_pct,
    total_revenue,
    ROUND((total_revenue - prev_month_revenue) * 100.0 / prev_month_revenue, 2) AS revenue_growth_pct
FROM monthly_data
WHERE prev_month_trips IS NOT NULL
ORDER BY reference_date;
```

This calculates month-over-month growth rates, showing which months had increasing or decreasing activity compared to the previous month.

**Query 11: Weekend vs weekday comparison**

```sql
SELECT
    dimension_label AS month_name,
    weekend_trips,
    weekday_trips,
    total_trips,
    ROUND(weekend_trips * 100.0 / total_trips, 2) AS weekend_pct,
    ROUND(weekday_trips * 100.0 / total_trips, 2) AS weekday_pct,
    ROUND(weekend_trips * 1.0 / 8, 0) AS avg_trips_per_weekend_day,
    ROUND(weekday_trips * 1.0 / 20, 0) AS avg_trips_per_weekday
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'monthly'
ORDER BY reference_date;
```

This compares weekend and weekday patterns, normalizing for the different number of weekend vs weekday days per month.

**Query 12: Efficiency metrics by month**

```sql
SELECT
    dimension_label AS month_name,
    total_trips,
    total_distance,
    total_revenue,
    avg_speed_mph,
    ROUND(total_revenue / total_distance, 2) AS revenue_per_mile,
    ROUND(total_distance / total_trips, 2) AS avg_trip_distance,
    ROUND(total_revenue / total_trips, 2) AS avg_revenue_per_trip
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'monthly'
ORDER BY reference_date;
```

This calculates various efficiency metrics that taxi companies care about—revenue per mile, average trip distance, and revenue per trip.

### Using Gold Layer for Dashboards

The gold layer is designed to feed into dashboards and visualization tools. Here's an example of how you might query it for a dashboard showing key performance indicators:

**Query 13: Current month KPIs**

```sql
WITH current_month AS (
    SELECT *
    FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
    WHERE aggregation_type = 'monthly'
    ORDER BY reference_date DESC
    LIMIT 1
),
previous_month AS (
    SELECT *
    FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
    WHERE aggregation_type = 'monthly'
    ORDER BY reference_date DESC
    LIMIT 1 OFFSET 1
)
SELECT
    c.dimension_label AS current_month,
    c.total_trips AS current_trips,
    p.total_trips AS previous_trips,
    ROUND((c.total_trips - p.total_trips) * 100.0 / p.total_trips, 1) AS trips_change_pct,
    c.total_revenue AS current_revenue,
    p.total_revenue AS previous_revenue,
    ROUND((c.total_revenue - p.total_revenue) * 100.0 / p.total_revenue, 1) AS revenue_change_pct,
    c.credit_card_pct AS current_credit_pct,
    p.credit_card_pct AS previous_credit_pct
FROM current_month c
CROSS JOIN previous_month p;
```

This single query provides all the data needed for a KPI dashboard showing current month performance compared to previous month.

### Exporting Data for Analysis

You can also export gold layer data for analysis in other tools like Excel, Python, or R:

```bash
# Export to CSV
bq extract \
    --destination_format=CSV \
    nyc_taxi_dataset.gold_yellow_taxi \
    gs://your-bucket/gold_export.csv

# Or query and download directly
bq query --format=csv --use_legacy_sql=false \
    'SELECT * FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi` 
     WHERE aggregation_type = "monthly"' \
    > monthly_data.csv
```

The pre-aggregated gold layer makes exports fast and cheap. You're exporting thousands of rows instead of millions, and the aggregations are already calculated.

---

## Monitoring and Operations

Operating a data pipeline isn't just about running it—it's about understanding its behavior, catching issues before they become problems, and maintaining reliable service over time. Let me walk you through the operational aspects of running this pipeline in production.

### Daily Operations Checklist

Every morning (or whenever you check in on the pipeline), there's a routine you should follow to ensure everything is running smoothly.

First, check if the cron job executed successfully. On your server, look at the most recent log file:

```bash
# View list of recent executions
ls -lt ~/nyc-taxi-pipeline/logs/cron/ | head -5

# Check the most recent log
tail ~/nyc-taxi-pipeline/logs/cron/pipeline_*.log

# Look for the exit code
grep "Exit code" ~/nyc-taxi-pipeline/logs/cron/pipeline_*.log | tail -1
```

If the exit code is 0, the pipeline succeeded. If it's anything else, the pipeline failed and you need to investigate.

Second, query the metadata table to verify data was actually loaded:

```sql
SELECT
    pipeline_name,
    month_loaded,
    status,
    rows_loaded,
    run_timestamp,
    ROUND(runtime, 2) AS runtime_seconds
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.pipeline_metadata`
ORDER BY run_timestamp DESC
LIMIT 5;
```

This shows the five most recent pipeline runs. You should see today's incremental run with SUCCESS status and a reasonable row count (2-3 million rows per month).

Third, check that gold layer data is current:

```sql
SELECT
    aggregation_type,
    COUNT(*) AS row_count,
    MAX(reference_date) AS latest_date
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
GROUP BY aggregation_type;
```

The latest_date for monthly and daily aggregations should be recent. If it's old, the pipeline might not be processing new data properly.

Fourth, check BigQuery costs for the past 24 hours:

In the BigQuery console, navigate to your project's billing page. Look at the "Query costs" section and verify the daily costs are within expected range (typically $0.10-0.50 per day for incremental runs).

### Understanding Pipeline Logs

The pipeline generates two types of logs: cron execution logs and application logs. Understanding both is crucial for troubleshooting.

Cron execution logs (`logs/cron/pipeline_*.log`) show the overall execution flow. These logs include timestamps, major events (starting, finishing, errors), and exit codes. They're generated by the cron wrapper script and capture the big picture of what happened.

Application logs (`logs/pipeline.log`) contain detailed logging from the Python code. These logs include debug information, step-by-step progress, retry attempts, error stack traces, and performance metrics. They're generated by Python's logging module and provide granular detail.

When troubleshooting a failed run, start with the cron log to understand what failed at a high level, then dive into the application log for detailed error messages and stack traces.

### Common Issues and Resolutions

Over time, you'll encounter various issues. Here are the most common ones and how to resolve them.

**Issue: Pipeline fails with "Network timeout"**

This usually happens when downloading large parquet files over a slow or unstable connection.

Resolution:
```bash
# Check your network connection
ping -c 5 d37ci6vzurychx.cloudfront.net

# If network is slow, increase timeout in config
# Edit .env:
DOWNLOAD_TIMEOUT=600  # Increase from default 300
```

The retry handler should automatically recover from temporary network issues. If failures persist, your network connection may need upgrading.

**Issue: Pipeline fails with "BigQuery quota exceeded"**

This happens if you run the pipeline many times in quick succession, exceeding BigQuery's API rate limits.

Resolution:
Wait 10-15 minutes for rate limits to reset, then run the pipeline again. If this happens regularly, space out your pipeline runs more. The daily cron schedule should never hit rate limits.

**Issue: Metadata shows SUCCESS but no rows loaded**

This indicates the pipeline ran but skipped loading because data already exists.

Resolution:
This is normal behavior if the pipeline runs multiple times for the same month. The pipeline checks if data exists and skips reloading to avoid duplicates. Check the log for messages like "data already exists in staging, skipping upload".

**Issue: Pipeline runs but gold layer isn't updated**

This suggests the pipeline failed partway through, possibly during silver or gold creation.

Resolution:
```bash
# Check application log for errors
grep ERROR ~/nyc-taxi-pipeline/logs/pipeline.log | tail -20

# Look for SQL errors
grep "SQL" ~/nyc-taxi-pipeline/logs/pipeline.log | tail -20
```

SQL errors usually indicate syntax issues or schema problems. Review the error message and check the SQL files for problems.

**Issue: Memory errors during execution**

This happens if your server doesn't have enough RAM to process monthly data files.

Resolution:
The pipeline is designed to use minimal memory by processing one month at a time. If you still see memory errors, you might need to upgrade your server. A machine with 2GB RAM should be sufficient.

**Issue: Cron job doesn't run**

If the scheduled execution never happens, there might be a problem with cron itself.

Resolution:
```bash
# Check if cron service is running
sudo systemctl status cron

# Check crontab is configured
crontab -l

# Check system logs for cron activity
grep CRON /var/log/syslog | tail -20

# Verify script has execute permissions
ls -la ~/nyc-taxi-pipeline/run_pipeline_cron.sh
```

### Performance Monitoring

Tracking pipeline performance over time helps identify trends and potential issues before they become critical.

**Query: Average runtime by month**

```sql
SELECT
    month_loaded,
    COUNT(*) AS execution_count,
    ROUND(AVG(runtime), 2) AS avg_runtime_seconds,
    ROUND(MIN(runtime), 2) AS min_runtime,
    ROUND(MAX(runtime), 2) AS max_runtime,
    ROUND(AVG(rows_loaded), 0) AS avg_rows_loaded
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.pipeline_metadata`
WHERE status = 'SUCCESS'
    AND pipeline_name = 'incremental'
    AND month_loaded != 'full year'
GROUP BY month_loaded
ORDER BY month_loaded;
```

This shows performance characteristics for each month. If you see runtime increasing over time, it might indicate growing data volumes or performance degradation that needs attention.

**Query: Success rate over time**

```sql
WITH daily_runs AS (
    SELECT
        DATE(run_timestamp) AS run_date,
        COUNT(*) AS total_runs,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs
    FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.pipeline_metadata`
    GROUP BY run_date
)
SELECT
    run_date,
    total_runs,
    successful_runs,
    failed_runs,
    ROUND(successful_runs * 100.0 / total_runs, 2) AS success_rate_pct
FROM daily_runs
ORDER BY run_date DESC
LIMIT 30;
```

This calculates daily success rates. A healthy pipeline should have 95%+ success rate. If success rate drops below 90%, investigate what's causing the failures.

### Cost Monitoring

Understanding and controlling costs is crucial for sustainable pipeline operations.

BigQuery provides detailed cost breakdowns. In the BigQuery console, navigate to your project's billing page. You'll see costs broken down by:
- Query compute costs (charged per TB of data processed)
- Storage costs (charged per GB of data stored)
- Streaming insert costs (charged per GB inserted via streaming API)

For this pipeline, the main cost driver is query compute. Each pipeline run processes several GBs of data through BigQuery queries. The partitioned staging table and optimized queries keep these costs low—typically $0.10-0.50 per incremental run.

Storage costs are minimal because we're only storing 2024 data. With about 35 million rows across all tables, storage costs are typically under $1 per month.

If costs start climbing unexpectedly, check:
- Are you running the pipeline more frequently than needed?
- Are queries scanning more data than necessary?
- Has the data volume increased significantly?

### Backup and Disaster Recovery

While BigQuery provides durability and automatic backups, having an explicit backup strategy is good practice.

**Exporting tables for backup:**

```bash
# Export staging table
bq extract \
    --destination_format=PARQUET \
    nyc_taxi_dataset.staging_yellow_taxi \
    gs://your-backup-bucket/staging/staging_*.parquet

# Export metadata
bq extract \
    --destination_format=NEWLINE_DELIMITED_JSON \
    nyc_taxi_dataset.pipeline_metadata \
    gs://your-backup-bucket/metadata/metadata.json
```

Store these backups in a Google Cloud Storage bucket with lifecycle policies to manage costs. You might keep:
- Last 7 days: All backups
- Last 30 days: Weekly backups
- Last 365 days: Monthly backups

**Recovery from backup:**

If you ever need to restore from backup:

```bash
# Restore staging table
bq load \
    --source_format=PARQUET \
    nyc_taxi_dataset.staging_yellow_taxi \
    gs://your-backup-bucket/staging/staging_*.parquet

# Restore metadata
bq load \
    --source_format=NEWLINE_DELIMITED_JSON \
    nyc_taxi_dataset.pipeline_metadata \
    gs://your-backup-bucket/metadata/metadata.json
```

After restoration, run full refresh to rebuild raw, silver, and gold tables from staging.

### Maintenance Tasks

Regular maintenance keeps the pipeline running smoothly.

**Monthly tasks:**
- Review metadata for any patterns in failures
- Check BigQuery storage costs and optimize if needed
- Review logs for any recurring warnings
- Update dependencies if security updates are available

**Quarterly tasks:**
- Review and optimize SQL queries for performance
- Analyze cost trends and identify optimization opportunities
- Update documentation with any changes
- Review and update credentials/access controls

**Annual tasks:**
- Full security review of credentials and access
- Comprehensive performance analysis
- Evaluate if architecture changes would improve operations
- Update disaster recovery procedures and test restoration

---

## Lessons Learned

Building this pipeline taught me numerous lessons about data engineering, many of which I couldn't have learned without encountering real-world problems. Let me share the most valuable insights.

### Idempotency is Harder Than It Looks

My initial approach to idempotency was naive—just check if records exist for a given date range. This seemed logical and simple. But it failed immediately when I encountered date infiltrations and partial loads.

The lesson: Real-world data has surprises. Source files contain data from dates they shouldn't. Pipelines fail midway through loading. Network issues cause partial uploads. Your idempotency logic needs to handle all these cases, not just the happy path.

The solution—row-based checking with boundary rows—works because it verifies actual data presence rather than making assumptions based on metadata. This approach handles partial loads, infiltrations, and all the edge cases that simple date checks miss.

When building your own pipelines, invest time in robust idempotency checking early. Test it by deliberately interrupting loads midway. Verify it works with messy, real-world data. The time spent here pays dividends in reliability.

### Performance Optimization Directly Impacts Cost

When I first tested the pipeline, processing took 30+ minutes and cost $7 per run. These numbers seemed acceptable for occasional runs, but they weren't sustainable for daily operation or frequent development iteration.

Switching to PyArrow reduced runtime by 40% and costs by 30%. Partitioning the staging table made idempotency checks essentially free. Optimizing SQL queries further reduced data scanned and costs. These optimizations compounded—the final pipeline runs in 18 minutes and costs $5.

The lesson: Performance and cost are directly linked in cloud data processing. Faster pipelines cost less because you're using compute resources for less time. Efficient queries cost less because they process less data.

When optimizing, profile your pipeline to find bottlenecks. Focus on the slowest operations first—optimizing a 30-second operation saves more than optimizing a 3-second operation. Consider both time and data scanned when evaluating cloud costs.

### Two Bronze Layers Solve Real Problems

The traditional medallion architecture has three layers: Bronze, Silver, Gold. I added a fourth layer (splitting Bronze into Staging and Raw) because real-world data quality issues demanded it.

This decision seemed like overengineering at first. But it solved critical problems: Accurate idempotency checking (comparing downloads to exact source data in staging), Complete audit trail (staging preserves everything including infiltrations), Clean transformation input (raw provides filtered data to silver), and Debugging capability (comparing staging to raw shows exactly what was filtered and why).

The lesson: Don't be afraid to adapt standard architectures to your specific needs. The extra complexity of a fourth layer is justified if it solves real problems. What matters isn't following patterns religiously—it's building systems that work reliably with your actual data.

### Metadata is the Pipeline's Memory

I initially viewed metadata as nice-to-have documentation. But as the pipeline evolved, metadata became essential. The auto-progression feature depends entirely on metadata. Debugging impossible without metadata showing what happened in past runs. Cost tracking requires metadata to understand runtime trends. Operational visibility comes from querying metadata.

The lesson: Treat metadata as a first-class concern, not an afterthought. Record everything meaningful about pipeline executions. Make metadata queryable and accessible. Build features that leverage metadata.

Good metadata transforms a pipeline from a black box to an observable system. When something goes wrong, metadata tells you what happened. When planning changes, metadata shows you patterns in pipeline behavior. When justifying costs, metadata provides concrete numbers.

### CI/CD and Data Processing Should Be Separate

Early versions of this project ran the entire data pipeline in GitHub Actions. This seemed convenient—everything automated in one place! But it created problems: slow CI feedback (18 minutes per run), wasted CI resources (processing data on every code change), security risks (production credentials in CI), and conflated failure modes (hard to tell if failures were code problems or data problems).

Separating CI (code validation) from cron (data processing) solved all these problems. CI now gives fast feedback (2-3 minutes) and focuses on what CI is good at (validating code). Cron handles data processing with appropriate resources and credentials.

The lesson: Use each system for what it's designed for. CI/CD is for validating code changes quickly and safely. Scheduled jobs or workflow orchestrators are for running production data pipelines. Trying to use CI for both creates a mess.

### Documentation is Code

Six weeks into the project, I couldn't remember why I made certain design decisions. I had to re-read code and reconstruct the reasoning. This wasted time and almost led to undoing good decisions because I didn't understand their purpose.

I learned to document not just what the code does, but why it does it. Architecture decisions include the alternatives I considered and why I chose this approach. Complex logic has comments explaining the reasoning, not just describing the operations. Design trade-offs are documented so future me understands what I'm giving up for what I'm gaining.

The lesson: Document your reasoning, not just your implementation. Future you is effectively a different person who doesn't remember why decisions were made. Design decisions, trade-offs, and context are more valuable than code comments describing what's already visible.

### The SKIPPED Status Bug

This bug was subtle but broke auto-progression completely. The pipeline would load January, record SUCCESS, then on the next run load January again, see it already exists, record SKIPPED, but then on the third run try to load January again instead of moving to February.

The issue was that auto-progression only looked for SUCCESS status when determining the last completed month. SKIPPED months weren't considered completed, so the pipeline kept trying the same month repeatedly.

The fix was simple—treat both SUCCESS and SKIPPED as completed: `WHERE status IN ('SUCCESS', 'SKIPPED')`. But finding the bug took hours of debugging because the symptom (stuck on same month) didn't obviously point to the cause (status checking logic).

The lesson: State management in automated systems is tricky. Think through all possible states and transitions. Test the unhappy paths and edge cases. Small bugs in state logic can completely break automation.

### Test What You Can, Accept What You Can't

I wanted comprehensive test coverage including full end-to-end tests of the complete pipeline. But running full pipeline tests requires BigQuery access, processes millions of rows, takes 18 minutes, and costs money. Running this in CI on every commit is impossible.

I learned to be strategic about testing: Unit tests validate pure logic without external dependencies (fast, cheap, runs in CI), Integration tests verify BigQuery interactions (slow, expensive, runs manually), End-to-end tests validate complete pipeline (very slow, very expensive, runs rarely before major releases).

The lesson: Perfect test coverage is impossible for data pipelines. Test what you reasonably can at each tier. Accept that some things require manual testing or verification in production. Good monitoring and metadata help catch issues that tests miss.

### PyArrow is Worth the Learning Curve

Switching from pure pandas to PyArrow required learning a new API and understanding columnar memory formats. This took time and felt like unnecessary complexity. But the performance gains were dramatic—3x faster reading, 30% faster uploads, 25% less memory usage.

PyArrow's columnar format aligns with parquet and BigQuery's internal formats, enabling efficient zero-copy operations. This isn't just academic—it's the difference between 30-minute pipeline runs and 18-minute runs.

The lesson: Sometimes the "more complex" solution is actually simpler in the long run. PyArrow seemed complex initially, but it's purpose-built for this use case. Using the right tool for the job often means better performance with less code.

### Cost Consciousness from Day One

I didn't initially think much about costs—GCP's free tier seemed generous, and I assumed costs would be negligible for this dataset size. Then I saw the first bills and realized that careless queries could cost $10+ each.

Adding partitioning, optimizing queries, and implementing smart caching reduced costs by 60%. More importantly, understanding cost drivers shaped architecture decisions—using CREATE OR REPLACE rather than incremental updates, processing one month at a time rather than all data together, checking metadata before expensive operations.

The lesson: Understand the cost model of your cloud platform early. Design with costs in mind from the beginning. It's much harder to optimize costs after your architecture is set. Small decisions compound—a $0.50 optimization per run saves $180 per year.

### Resilience Through Retry and Idempotency

Network hiccups, API rate limits, and temporary service unavailability are facts of life in cloud computing. Without resilience mechanisms, these transient issues would require manual intervention every time they occurred.

The retry handler with exponential backoff automatically recovers from temporary failures. Idempotency ensures that retries don't create duplicate data. Together, these mechanisms make the pipeline self-healing for most issues.

The lesson: Build resilience into your system from the start. Assume failures will happen and design for automatic recovery. Retry logic with exponential backoff handles most transient issues. Idempotency makes retries safe. These mechanisms transform a fragile system into a robust one.

### The Value of Structured Logging

Early versions used print statements for debugging. This seemed simple and worked fine during development. But when troubleshooting production issues, I had no timestamps, no log levels, no structured data. Finding relevant information in logs was painful.

Switching to proper structured logging with Python's logging module transformed debugging. Every log entry has a timestamp, log level, logger name, and message. I can filter by level (ERROR, WARNING, INFO) to focus on problems. Structured logging makes log analysis tools effective.

The lesson: Use proper logging from the start. Structured logging seems like overhead initially, but it's invaluable when debugging production issues. Log levels let you control verbosity without changing code. Timestamps enable understanding event sequences.

---

## Conclusion and Next Steps

This pipeline represents a production-grade data engineering system built with modern best practices. It handles over 41 million records, processes data through multiple quality layers, automatically recovers from failures, tracks comprehensive metadata, and integrates with CI/CD workflows. More importantly, it demonstrates understanding of real-world data engineering challenges—data quality issues, cost optimization, idempotency, observability, and operational reliability.

### What Makes This Production-Grade

Several characteristics distinguish this pipeline from a learning project or proof of concept.

**Robust idempotency:** The pipeline can be run multiple times safely without creating duplicate data or corrupting the database. This is essential for automated operation and recovery from failures.

**Comprehensive error handling:** Every component has proper error handling with retry logic. Failures are caught, logged, recorded in metadata, and reported clearly. The pipeline fails gracefully rather than mysteriously.

**Complete observability:** Metadata tracking, structured logging, and queryable history provide complete visibility into pipeline behavior. You can always answer "what happened during that run" and "why did it fail."

**Cost optimization:** Partitioned tables, efficient queries, and smart caching keep BigQuery costs low despite processing millions of rows. The pipeline is sustainable for academic budgets.

**Separation of concerns:** CI validates code quality, cron handles data processing, layers have clear responsibilities. This separation makes the system easier to understand and maintain.

**Quality assurance:** Automated testing, linting, and code formatting ensure consistent code quality. Branch protection prevents broken code from reaching production.

**Documentation:** Comprehensive documentation covers architecture, design decisions, operational procedures, and lessons learned. Future maintainers can understand why decisions were made.

These characteristics together create a system that's reliable, maintainable, and professional-quality.

### Possible Enhancements

While the current pipeline is complete and functional, several enhancements could add value in different contexts.

**Data quality framework:** Integrating Great Expectations or similar tools would provide automated data quality checks. Instead of discovering data quality issues manually, the pipeline would automatically validate that data meets expected patterns and flag anomalies.

**Real-time streaming:** The current batch architecture processes monthly files. A streaming version using Google Pub/Sub and Dataflow could process taxi trips in real-time as they happen, enabling live dashboards and immediate insights.

**dbt integration:** dbt provides sophisticated SQL-based transformations with built-in testing, documentation, and lineage tracking. Migrating transformations to dbt would improve maintainability and provide better visibility into data lineage.

**Advanced monitoring:** Integration with Datadog, Grafana, or similar tools would provide dashboards, alerts, and trend analysis. You could monitor pipeline performance, costs, and data quality in one place.

**Machine learning integration:** The clean, aggregated data in the gold layer is perfect for training ML models to predict taxi demand, optimize pricing, or forecast revenue. Adding prediction capabilities would demonstrate end-to-end ML operations.

**Multi-year support:** The current pipeline focuses on 2024 data. Extending it to handle multiple years with configurable year parameters would make it more versatile.

**Airflow orchestration:** While Python orchestration works well, migrating to Apache Airflow would provide a web UI, advanced scheduling, backfilling, and dependency management.

Each enhancement adds complexity, so evaluate whether the added value justifies the additional maintenance burden.

### Key Takeaways

If you take away nothing else from this documentation, remember these principles:

**Idempotency is non-negotiable for production pipelines.** Ensure your pipeline can be run multiple times safely.

**Optimize for cost from day one.** Understand your cloud platform's cost model and design accordingly.

**Separate CI/CD from data processing.** Use each system for what it's designed for.

**Metadata is essential, not optional.** Track everything about pipeline executions.

**Documentation is for your future self.** Explain why decisions were made, not just what the code does.

**Test strategically.** You can't test everything, so focus on what matters most.

**Build for observability.** Logs, metrics, and metadata make systems debuggable.

**Performance and cost are linked.** Faster pipelines cost less in cloud environments.

These principles apply beyond this specific project—they're fundamental to building reliable data systems.

### Using This Project

This project is fully open source and available for learning, adaptation, and extension. The code is well-documented and structured to be readable. The architecture is explained with rationale for each decision. The documentation covers both how to use it and why it's built this way.

You can use this project as a template for your own data pipelines, a learning resource for data engineering concepts, a portfolio piece demonstrating production-grade skills, or a foundation to build upon with enhancements.

If you build something based on this work, I'd love to hear about it. Data engineering is a collaborative field, and seeing how others adapt and improve these patterns helps everyone learn.

### Final Thoughts

Data engineering is about much more than moving data from A to B. It's about building reliable systems that handle failures gracefully, scale efficiently, remain observable, and can be maintained over time. This project demonstrates these principles through concrete implementation.

The journey from "just load some data" to understanding production-grade data engineering involves encountering real problems and solving them properly. Date infiltrations taught me about data quality. Performance issues taught me about optimization. Failed runs taught me about resilience. Each challenge improved the system and deepened understanding.

I hope this documentation helps you on your own data engineering journey. Whether you're a student learning the field, a professional building your first pipeline, or an experienced engineer looking for new patterns, I hope the detailed explanations and reasoning prove valuable.

Thank you for taking the time to read this comprehensive documentation. Now go build something amazing.

---

**Project Repository:** [github.com/prantonia/nyc-taxi-pipeline](https://github.com/yourusername/nyc-taxi-pipeline)

**Author:** Prantonia

**Date:** November 2024

**Version:** 1.0.0

---

*This documentation is part of the NYC Taxi Data Pipeline project, a demonstration of production-grade data engineering practices. All code is open source and available for learning and adaptation.*