import streamlit as st
from scripts.run_etl import main

# Define pages
home_page = st.Page("pages/home.py", title="Home", icon="🏠")
dashboard_page = st.Page("pages/dashboard.py", title="Dashboard", icon="📊")
settings_page = st.Page("pages/settings.py", title="Settings", icon="⚙️")


# Create navigation
pg = st.navigation([home_page, dashboard_page, settings_page])
pg.run()
