# Two-Week Project Timeline

## Week 1

### Day 2: Data Ingestion and EDA

- What's working now:
  - Spark setup (docker-single worker)
  - Can write iceberg tables in minio bucket using pyspark
  - Nessie is working as a catalog
  - Postgres as backend for catalog persistence being populated
  - Dremio UI to write aggregation queries
  - Ingestion with Census API
- To DO
  - Setup EDA with spark notebook for data visualization
  - Look for other endpoints (there's quite a few in Census API)
  - Find areas of interest to focus on

### Day 3: Continue

- More EDA, and Research
- Gather Tarrif announcement data and implementation dates
- Finalize the product to focus (export import)

### Day 4: Data Validation

- Check for duplicates
- Find trends
- Think of visualization
- To DBT or NOT?

### Day 5: Orchestration/FastAPI

- Prefect
- FastAPI setup

### Day 6: Visualization

- FastAPI improvement
- Streamlit setup

### Day 7: Progress Review

- Streamlit improvement
- Review week’s progress
- Adjust plan if needed

---

## Week 2

### Day 8: Dashboard/Analyze trends

- Work on dashboard
- Analyze any trends in the data
- investigate baseline models

### Day 9-11

- Implement MLOPS (experiment tracking and depolyment) with mlflow,optuna,pytorch (or scikit-learn) and depoly with FastAPI

### Day 12: Results Analysis

- Analyze model outputs
- Prepare visualizations

### Day 13: Documentation

- Document methodology and findings
- Prepare project report

### Day 14: Final Review & Presentation

- Finalize deliverables
- Present results to stakeholders

### If there is time

- refactor spark setup with devcontainer (masters and multiple workers)
- setup spark jobs for data ingestion
