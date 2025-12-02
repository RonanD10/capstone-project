import streamlit as st
from scripts.run_etl import main_etl


# Get data from ETL pipeline
@st.cache_data(show_spinner="Running ETL pipeline...")
def load_data():
    return main_etl()


DATA = load_data()

# Define pages
home_page = st.Page("pages/home.py", title="Home", icon="🏠")
sports_comparison_page = st.Page("pages/sports_comparison.py", title="Comparison of Sports", icon="📊")
optimal_athlete_page = st.Page("pages/optimal_athlete.py", title="Build the Optimal Athlete!", icon="📊")
fun_facts_page = st.Page("pages/fun_facts.py", title="Fun Olympics Facts")


# Create navigation
pg = st.navigation([home_page, sports_comparison_page, optimal_athlete_page, fun_facts_page])
pg.run()
