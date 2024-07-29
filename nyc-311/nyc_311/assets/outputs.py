import geopandas as gpd
import plotly
import plotly.express as px
from datetime import datetime
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    asset
)
from dagster_duckdb import DuckDBResource
from . import constants

@asset(
    deps=["service_requests_table", "taxi_zones_table"],
    group_name="outputs",
)
def brooklyn_geodataframe(
    context: AssetExecutionContext,
    database: DuckDBResource
) -> MaterializeResult:
    query = f"""
        SELECT
            z.zone,
            sr.incident_zip,
            z.geometry
        FROM {constants.NYC_311_SERVICE_REQUESTS_TABLE} sr
        LEFT JOIN {constants.NYC_TAXI_ZONES_TABLE} z ON sr.borough = UPPER(z.borough) 
        WHERE z.borough = 'Brooklyn' AND z.geometry IS NOT NULL
        GROUP BY z.zone, sr.incident_zip, z.geometry
        ORDER BY sr.incident_zip ASC;
    """

    with database.get_connection() as conn:
        df = conn.execute(query).fetch_df()

    context.log.info(f"create GeoDataFrame and save it to {constants.BROOKLYN_GEODATAFRAME_FILE_PATH}")
    df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
    df = gpd.GeoDataFrame(df)

    with open(constants.BROOKLYN_GEODATAFRAME_FILE_PATH, 'w') as f:
        f.write(df.to_json())

    return MaterializeResult(
        metadata={
            "materialization_date": datetime.today().strftime('%Y-%m-%d')
        }
    )

@asset(
    deps=["service_requests_table", "taxi_zones_table"],
    group_name="outputs",
)
def manhattan_geodataframe(
    context: AssetExecutionContext,
    database: DuckDBResource
) -> MaterializeResult:
    query = f"""
        SELECT
            z.zone,
            sr.incident_zip,
            z.geometry
        FROM {constants.NYC_311_SERVICE_REQUESTS_TABLE} sr
        LEFT JOIN {constants.NYC_TAXI_ZONES_TABLE} z ON sr.borough = UPPER(z.borough) 
        WHERE z.borough = 'Manhattan' AND z.geometry IS NOT NULL
        GROUP BY z.zone, sr.incident_zip, z.geometry
        ORDER BY sr.incident_zip ASC;
    """

    with database.get_connection() as conn:
        df = conn.execute(query).fetch_df()

    context.log.info(f"create GeoDataFrame and save it to {constants.MANHATTAN_GEODATAFRAME_FILE_PATH}")
    df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
    df = gpd.GeoDataFrame(df)

    with open(constants.MANHATTAN_GEODATAFRAME_FILE_PATH, 'w') as f:
        f.write(df.to_json())

    return MaterializeResult(
        metadata={
            "materialization_date": datetime.today().strftime('%Y-%m-%d')
        }
    )

@asset(
    deps=["brooklyn_geodataframe"],
    group_name="outputs"
)
def brooklyn_map_html(context: AssetExecutionContext):
    df = gpd.read_file(constants.BROOKLYN_GEODATAFRAME_FILE_PATH)

    fig = px.choropleth_mapbox(df,
        geojson=df.geometry.__geo_interface__,
        locations=df.index,
        # if this is not commented out, it takes forever to render the map
        # color='zone',
        color_continuous_scale='Plasma',
        mapbox_style='carto-positron',
        center={'lat': 40.678, 'lon': -73.944},
        zoom=10,
        opacity=0.7,
        # labels={'zone': 'Borough zone'}
    )

    context.log.info(f"export map to {constants.BROOKLYN_MAP_FILE_PATH_HTML}")
    # I can save the plot as an HTML file and view it in a browser
    # python -m http.server -d nyc-311/data/outputs/ 8888
    plotly.offline.plot(fig, filename=constants.BROOKLYN_MAP_FILE_PATH_HTML)


@asset(
    deps=["manhattan_geodataframe"],
    group_name="outputs"
)
def manhattan_map_html(context: AssetExecutionContext):
    df = gpd.read_file(constants.MANHATTAN_GEODATAFRAME_FILE_PATH)

    fig = px.choropleth_mapbox(df,
        geojson=df.geometry.__geo_interface__,
        locations=df.index,
        # color='num_trips',
        color_continuous_scale='Plasma',
        mapbox_style='carto-positron',
        center={'lat': 40.758, 'lon': -73.985},
        zoom=11,
        opacity=0.7,
        # labels={'num_trips': 'Number of Trips'}
    )

    context.log.info(f"export map to {constants.MANHATTAN_MAP_FILE_PATH_HTML}")
    # I can save the plot as an HTML file and view it in a browser
    # python -m http.server -d nyc-311/data/outputs/ 8888
    plotly.offline.plot(fig, filename=constants.MANHATTAN_MAP_FILE_PATH_HTML)

@asset(
    deps=["service_requests_table"],
    group_name="outputs"
)
def facet_plot_html(
    context: AssetExecutionContext,
    database: DuckDBResource
):
    query = f"""
    WITH daily_counts AS (
    SELECT
      date_trunc('day', created_date) AS complaint_date,
      borough,
      COUNT(*) AS daily_complaints
    FROM
      data.main.service_requests
    GROUP BY
      date_trunc('day', created_date),
      borough
    )
    SELECT
      complaint_date,
      daily_complaints,
      borough
    FROM
      daily_counts
    ORDER BY
      complaint_date,
      daily_complaints DESC;
    """

    with database.get_connection() as conn:
        df = conn.execute(query).fetch_df()

    fig = px.line(df, x="complaint_date", y="daily_complaints", facet_col="borough",
      facet_col_wrap=3,
      title="Daily complaints over time, grouped by NYC borough"
    )
    # this changes only one facet plot, not all of them
    # fig.update_layout(xaxis_title="Date", yaxis_title="Complaints reported")

    # This changes each facet plot title from something like "borough=MANHATTAN"
    # to just "MANHATTAN"
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.for_each_xaxis(lambda a: a.update(title="Date"))
    fig.for_each_yaxis(lambda a: a.update(title="Complaints reported"))

    plotly.offline.plot(fig, filename=constants.FACET_PLOT_PATH_HTML)
