# cz4031-query-processor
## Connecting to DB
1) Make sure to add your password to database.ini
2) Please install these packages  
```
pip install psycopg2
```
```
pip install config
```

## explainQuery()
1) Takes the query as input calls the PostgreSQL Database and returns the query plan
2) Can return in text or json using parameter format
3) Please dont include the EXPLAIN keyword in the query

## Setting up Streamlit
1) Create new conda environment
2) Activate conda environment then run ``` pip install streamlit ```
3) Test if streamlit is working by running ``` streamlit hello ```. Streamlit's app should appear in a new tab in the web browser.

## Running Streamlit Application
```
streamlit run project.py
