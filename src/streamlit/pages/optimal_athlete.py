import streamlit as st
import pandas as pd
import numpy as np 
from src.streamlit.app import DATA


df = DATA 

sports = df["sport"].unique()
sports.sort()

st.set_page_config(layout="wide")


def get_events(sport, sex):
    sport_df = df[df["sport"] == sport]
    events = sport_df["event"].unique()
    mapper = {"Male": "Men", "Female": "Women"}
    events = [e for e in events if mapper[sex] in e]
    events.sort()
    return events


col0, col1, col2 = st.columns(3)

with col0:
    sex = st.selectbox(
        "Sex",
        ["Male", "Female"],
        index=None,
        placeholder="Select an option",
    )

with col1:
    sport = st.selectbox(
        "Sport",
        sports, 
        index=None,
        placeholder="Select an option",
    )

with col2:
    events = get_events(sport, sex)
    event = st.selectbox(
        "Event",
        events, 
        index=None,
        placeholder="Select an option",
    )


def get_avg(sport, event, is_optimal):
    """
    Return: average age, height, weight for sport, event; if is_optimal, then averages for Gold medalists
    """
    avg_df = df[(df["sport"] == sport) & (df["event"] == event)]
    if is_optimal:
        avg_age = avg_df[avg_df["medal"] == "Gold"]["age"].mean()
        avg_height = avg_df[avg_df["medal"] == "Gold"]["height_cm"].mean()
        avg_weight = avg_df[avg_df["medal"] == "Gold"]["weight_kg"].mean()
    else: 
        avg_age = avg_df["age"].mean()
        avg_height = avg_df["height_cm"].mean()
        avg_weight = avg_df["weight_kg"].mean()

    return (np.round(avg_age, 1), np.round(avg_height, 1), np.round(avg_weight, 1))


def perc_dif(value, avg_value):
    perc = 100 * ((value - avg_value) / avg_value)
    return np.round(perc, 1)


def display_card(sex, opt_age, avg_age, opt_height, avg_height, opt_weight, avg_weight):
    col0a, col1a, col2a, col3a = st.columns(4)
    col0a.metric("Sex", f"{sex}")
    col1a.metric("Age", f"{opt_age}", f"{perc_dif(opt_age, avg_age)}%")
    col2a.metric("Height", f"{opt_height} cm", f"{perc_dif(opt_height, avg_height)}%")
    col3a.metric("Weight", f"{opt_weight} kg", f"{perc_dif(opt_weight, avg_weight)}%")


avg_age_optimal = get_avg(sport, event, True)[0]
avg_height_optimal = get_avg(sport, event, True)[1]
avg_weight_optimal = get_avg(sport, event, True)[2]

avg_age = get_avg(sport, event, False)[0]
avg_height = get_avg(sport, event, False)[1]
avg_weight = get_avg(sport, event, False)[2]

display_card(sex, avg_age_optimal, avg_age, avg_height_optimal, avg_height, avg_weight_optimal, avg_weight)
