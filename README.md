# Capstone Project

This is a Capstone project for the Digital Futures Data Engineering Academy. 

This is a repository for the code to run a Streamlit app, which runs an ETL pipeline, which loads data to visualisations in the app. The data used is Olympic data since the beginning of the modern summer and winter Olympics (1896) until 2016. 

Data source: https://www.kaggle.com/datasets/heesoo37/120-years-of-olympic-history-athletes-and-results?select=athlete_events.csv

Usage: 

1. ```pip install -e .``` installs the requirements and configures the scripts for running ETL and app
2. To run the ETL pipeline, enter ```run_etl```
3. To run the app (which also runs the ETL pipeline), enter ```run_app```
4. To run tests, enter ```run_test <test_config>```, where ```<test_config>``` can be ```lint```, ```unit```, ```cov```,```component```, ```integration```, ```e2e```, ```all```

![alt text](https://github.com/RonanD10/capstone-project/blob/main/images/homepage.png)


