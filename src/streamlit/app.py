import streamlit as st


# Define pages
home_page = st.Page("pages/home.py", title="Home", icon="🏠")
medal_stats_page = st.Page("pages/medal_stats.py", title="Medal Records", icon="🏅")
optimal_athlete_page = st.Page("pages/optimal_athlete.py", title="Build the Optimal Athlete!", icon="🏋🏼‍♂️")
fun_facts_page = st.Page("pages/fun_facts.py", title="Fun Olympics Facts", icon="💡")


# Create navigation
pg = st.navigation([home_page, medal_stats_page, optimal_athlete_page, fun_facts_page])
pg.run()
