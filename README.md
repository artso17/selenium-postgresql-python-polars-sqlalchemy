# Data Ingestion of Extract Load Transform Pipeline Process Using Python and PostgreSQL

## Project Brief
The Project developed to simulate Data Ingestion of Extract Load Transform Pipeline Process Using Python and PostgreSQL. The case study is how to extract data from e-commerce platform Tokopedia using Web Scraping method and load it to Data Lake. In Transformation step, the raw data extracted from data lake need to be cleaned from missing value and duplicated one and perform feature engineering then normalize columns that have multiple value to have only single value. Finally, the cleaned and transformed data needs to be loaded to Data Warehouse. 

## Tools and Modules
- Python
- PostgreSQL
- Docker
- PGAdmin4
- Polars
- SQLAlchemy
- Tqdm
- Psycopg2
- Selenium

## Success Criteria
- Design Entity Relationship Diagram (ERD)
- Develop Object Relational Mapping  (ORM)
- Develop Web Scraping Script
- Develop ETL Script 
- Simulate the Process

## Result
- Designed ERD in PGAdmin4 `./media/keyboard_ERD.png`
- Developed ORM in `./models.py`
- Developed Web Scraping and ETL script in `./webscrap_etl.py`
- Simulated the Process