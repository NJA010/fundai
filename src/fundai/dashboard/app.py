import streamlit as st
import plotly.express as px
import pandas as pd
from fundai import DatabaseClient, load_config

db = DatabaseClient(load_config())
df = db.read_df("select * from property_listing")
print(df)
