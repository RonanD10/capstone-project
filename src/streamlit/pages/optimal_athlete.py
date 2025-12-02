import streamlit as st
import pandas as pd
from src.streamlit.app import DATA


df = DATA
summer = df[df["season"] == "Summer"]
winter = df[df["season"] == "Winter"]
# physical_sports = [""]
# old_sports = 
# historic_sports = 
# summer_physical = summer[[""]

age_by_sport_summer = summer.groupby("sport").mean("age")["age"].sort_values(ascending=False)
age_by_sport_winter = winter.groupby("sport").mean("age")["age"].sort_values(ascending=False)
avg_gold_age_summer = summer[summer["medal"] == "Gold"].groupby("year").mean("age")["age"]


def show_graphs():
    st.bar_chart(age_by_sport_summer, sort="age")
    st.bar_chart(age_by_sport_winter, sort="age")
    st.line_chart(avg_gold_age_summer)


show_graphs()