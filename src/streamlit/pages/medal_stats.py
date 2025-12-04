import streamlit as st
import pandas as pd
import plotly.express as px


st.title("🏅 Medal Records")

# Load data
df = pd.read_csv("data/processed/transformed_data.csv")

summer = df[df["season"] == "Summer"]
winter = df[df["season"] == "Winter"]

summer_medal_count = (
    summer
    .groupby("country")["medal"]
    .count()
    .reset_index(name="medal_count")
    .sort_values(by="medal_count", ascending=False)
)

winter_medal_count = (
    winter
    .groupby("country")["medal"]
    .count()
    .reset_index(name="medal_count")
    .sort_values(by="medal_count", ascending=False)
)

summer_and_winter_medal_count = (
    df
    .groupby("country")["medal"]
    .count()
    .reset_index(name="medal_count")
    .sort_values(by="medal_count", ascending=False)
)

summer_gold_medal_count = (
    summer[summer["medal"] == "Gold"]
    .groupby("country")["medal"]
    .count()
    .reset_index(name="medal_count")
    .sort_values(by="medal_count", ascending=False)
)

winter_gold_medal_count = (
    winter[winter["medal"] == "Gold"]
    .groupby("country")["medal"]
    .count()
    .reset_index(name="medal_count")
    .sort_values(by="medal_count", ascending=False)
)

summer_athlete_gold_medal_count = (
    summer[summer["medal"] == "Gold"]
    .groupby("name")["medal"]
    .count()
    .reset_index(name="medal_count")
    .sort_values(by="medal_count", ascending=False)
)

winter_athlete_gold_medal_count = (
    winter[winter["medal"] == "Gold"]
    .groupby("name")["medal"]
    .count()
    .reset_index(name="medal_count")
    .sort_values(by="medal_count", ascending=False)
)

fig1 = px.bar(
    summer_medal_count.head(10), 
    title="Top 10 Countries with Most Total Summer Medals",
    color=(["red",] * 10), color_discrete_map="identity", y="medal_count", x="country"
    )

fig2 = px.bar(
    summer_gold_medal_count.head(10), 
    title="Top 10 Countries with Most Total Summer Gold Medals",
    color=(["red",] * 10), color_discrete_map="identity", y="medal_count", x="country"
    )

fig3 = px.bar(
    winter_medal_count.head(10), 
    title="Top 10 Countries with Most Total Winter Medals",
    y="medal_count", x="country"
    )

fig4 = px.bar(
    winter_gold_medal_count.head(10), 
    title="Top 10 Countries with Most Total Winter Gold Medals",
    y="medal_count", x="country"
    )

fig5 = px.bar(
    summer_and_winter_medal_count.head(10), 
    title="Top 10 Countries with Most Total Summer and Winter Medals",
     color=(["green",] * 10), color_discrete_map="identity", y="medal_count", x="country"
    )

fig6 = px.choropleth(
    summer_medal_count,
    locations="country",
    locationmode="country names",   # or "ISO-3"
    color="medal_count",
    hover_name="country",
    color_continuous_scale="YlOrRd",
    title="Total Summer Medals Per Country",
)

fig6.update_layout(
    geo=dict(showframe=False, showcoastlines=True)
)

fig7 = px.choropleth(
    winter_medal_count,
    locations="country",
    locationmode="country names",   # or "ISO-3"
    color="medal_count",
    hover_name="country",
    title="Total Winter Medals Per Country",
)

fig7.update_layout(
    geo=dict(showframe=False, showcoastlines=True)
)

fig8 = px.bar(
    summer_athlete_gold_medal_count.head(10), 
    title="Top 10 Athletes with Most Total Summer Gold Medals",
    y="medal_count", x="name",
    color=(["red",] * 10), color_discrete_map="identity",
)

fig9 = px.bar(
    winter_athlete_gold_medal_count.head(10), 
    title="Top 10 Athletes with Most Total Winter Gold Medals",
    y="medal_count", x="name",
)

st.plotly_chart(fig1)
st.plotly_chart(fig2)
st.plotly_chart(fig3)
st.plotly_chart(fig4)
st.plotly_chart(fig5)
st.plotly_chart(fig6)
st.plotly_chart(fig7)
st.plotly_chart(fig8)
st.plotly_chart(fig9)