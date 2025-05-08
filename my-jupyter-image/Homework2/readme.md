# Homework 2 – MIMIC-III Analysis

This project explores healthcare data using both SQL (via DuckDB) and NoSQL (via AWS Keyspaces/Cassandra) to answer three analysis questions from the MIMIC-III dataset.

## Structure

1. **Part I – SQL (DuckDB)**  
   Load MIMIC-III `.csv` files using DuckDB and answer:
   - Drug usage by ethnicity
   - Top procedures by age group
   - ICU length of stay differences by gender and ethnicity

2. **Part II – NoSQL (Cassandra)**  
   Create optimized tables in Cassandra for the same questions using AWS Keyspaces and `boto3` for authentication. Insert SQL results as records and re-query to validate.

## How to Run

1. **Launch Jupyter Notebook (Docker-based)**  
   Ensure your environment (e.g., `my-jupyter-image`) is running and `.aws/credentials` are correctly mounted.

2. **Run cells sequentially:**
   - **DuckDB Setup:** Load tables and run SQL queries
   - **Cassandra Setup:** Connect using TLS and `boto3`, create keyspace, create tables, and insert data from SQL results
   - **Query Cassandra:** Run `SELECT` statements to verify data is stored and structured properly

3. **AWS Keyspaces Credentials:**  
   Must remain private and never committed. Stored in `.aws/credentials`.

## Expected Outputs

- **SQL (DuckDB)**:  
   Tables displaying:
   - The top drug per ethnicity
   - The top 3 procedures per age group
   - Average ICU stay by gender and ethnicity

- **Cassandra**:  
   `SELECT *` results from:
   - `drug_summary_result`
   - `procedure_summary`
   - `icu_summary_by_ethnicity`  
   ...should match the structure and values seen in DuckDB outputs.

## Notes

- DuckDB queries use basic joins, grouping, and ordering (no window functions).
- Cassandra inserts use pandas DataFrames from SQL results.
- Post-extraction analysis (e.g., sorting, filtering) is done in Python.


