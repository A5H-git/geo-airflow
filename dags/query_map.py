from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import matplotlib.pyplot as plt
import requests
import pandas as pd
import geopandas as gpd
import numpy as np

from airflow.providers.sqlite.hooks.sqlite import SqliteHook

FIELDS = ["name", "population", "continents", "latlng", "area", "cca3"]
REQUEST_URL = f"https://restcountries.com/v3.1/all?fields={','.join(FIELDS)}"
OUTPUT_FOLDER = "/home/skytrain/airflow/data"
WORLD_MAP_PATH = (
    "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
)

DATABASE_CONN_ID = "sql_conn"


@dag(
    dag_id="query_map",
    schedule="@daily",
    catchup=False,
)
def query_map_dag():
    @task
    def make_request():
        response = requests.get(REQUEST_URL)
        return response.json()

    @task
    def clean_data(data):
        raw_df = pd.json_normalize(data)

        cleaned_df = pd.DataFrame()
        cleaned_df["name"] = raw_df["name.common"]
        cleaned_df["area"] = raw_df["area"]
        cleaned_df["country_code"] = raw_df["cca3"]
        cleaned_df["population"] = raw_df["population"]

        # 1. Only multiple continents is Europe + Asia, convert to Eurasia
        cont_filter = raw_df["continents"].apply(lambda x: len(x) > 1)
        raw_df.loc[cont_filter, "continents"] = raw_df.loc[
            cont_filter, "continents"
        ].apply(lambda _: ["Eurasia"])

        # 2. Convert to single instances
        cleaned_df["continent"] = raw_df["continents"].apply(lambda x: x[0])
        cleaned_df[["latitude", "longitude"]] = pd.DataFrame(
            raw_df["latlng"].tolist(), index=raw_df.index
        )

        return cleaned_df

    @task
    def transform_data(df: pd.DataFrame):
        df["pop_density"] = np.log1p(df["population"] / df["area"])
        return df

    @task
    def save_data(df: pd.DataFrame):
        df.to_csv(f"{OUTPUT_FOLDER}/countries.csv", index=False)

    @task
    def visualize_data(df: pd.DataFrame):
        world = gpd.read_file(WORLD_MAP_PATH)
        world = world.rename(columns={"ADM0_A3": "country_code"})
        world = world.merge(
            df, how="left", left_on="country_code", right_on="country_code"
        )
        world["pop_density"] = np.log1p(world["pop_density"])

        fig, ax = plt.subplots()  # figsize=(10, 10))
        ax = world.plot(column="pop_density", cmap="plasma", ax=ax)
        ax.axis("off")
        ax.set_title("Population Density of Countries")
        fig.savefig(f"{OUTPUT_FOLDER}/world_map_density.png")

    create_countries_table = SQLExecuteQueryOperator(
        task_id="create_countries_table",
        sql="""
        CREATE TABLE IF NOT EXISTS countries (
            name TEXT,
            area REAL,
            country_code TEXT,
            population INTEGER,
            pop_density REAL,
            latitude REAL,
            longitude REAL
        );
        """,
        conn_id=DATABASE_CONN_ID,
    )

    @task
    def load_to_sql(df: pd.DataFrame):
        hook = SqliteHook(DATABASE_CONN_ID)
        df.to_sql(
            name="countries", con=hook.get_conn(), if_exists="replace", index=False
        )

    simple_query = SQLExecuteQueryOperator(
        task_id="simple_query",
        conn_id=DATABASE_CONN_ID,
        sql="""
        SELECT 
            continent,
            SUM(population) AS tot_pop,
            AVG(pop_density) AS avg_density
        FROM 
            countries
        GROUP BY
            continent
        ORDER BY 
            avg_density DESC;
        """,
    )

    data = make_request()
    df = clean_data(data)
    df = transform_data(df)

    save_data(df)
    visualize_data(df)

    create_countries_table >> load_to_sql(df) >> simple_query


query_map_dag()
