import os
import subprocess
from pyspark.sql import SparkSession
import builtins
import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import folium
from folium.plugins import MarkerCluster
from pyspark.sql.functions import (
    hour, avg, count, desc, col, round, sum as _sum, when, dayofweek, date_format, unix_timestamp
)
import json

# Set default renderer
pio.renderers.default = 'notebook'

# Monthly processing
data_dir = "hdfs://namenode:8020/nyc_taxi/"
local_script = "get_plots_month.py"
parquet_files = [f"yellow_tripdata_2024-{str(m).zfill(2)}.parquet" for m in range(1, 13)]

for file in parquet_files:
    print(f"📊 Generating plots for {file}...")
    subprocess.run(["python", local_script, file])




# ================================================
# Now merge all data and generate combined plots


print("📦 Merging all Parquet files...")

spark = SparkSession.builder.appName("NYC Taxi Combined Analysis").getOrCreate()

df = spark.read.parquet(*[f"hdfs://namenode:8020/nyc_taxi/{f}" for f in parquet_files])

base_dir = "./saved_plots/all"
os.makedirs(base_dir, exist_ok=True)

df_clean = df.filter(
    (df['passenger_count'] > 0) &
    (df['trip_distance'] > 0) &
    (df['fare_amount'] > 0)
).withColumn("pickup_hour", hour(df["tpep_pickup_datetime"]))

# 1. Hourly Distribution
trip_hour_df = df_clean.groupBy("pickup_hour").agg(count("*").alias("trip_count")) \
    .orderBy("pickup_hour").toPandas()
fig1 = px.bar(trip_hour_df, x='pickup_hour', y='trip_count',
              title='Taxi Trip Count by Hour of Day (All Months)',
              color='trip_count', color_continuous_scale='Blues')
fig1.write_html(os.path.join(base_dir, "hourly_dist.html"))

# 2. Weekly Pattern
df_day = df_clean.withColumn("day_of_week", date_format("tpep_pickup_datetime", "E"))
trip_day_df = df_day.groupBy("day_of_week").agg(count("*").alias("trip_count")).toPandas()
days_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
trip_day_df['day_of_week'] = pd.Categorical(trip_day_df['day_of_week'], categories=days_order, ordered=True)
trip_day_df = trip_day_df.sort_values('day_of_week')
fig2 = px.bar(trip_day_df, x='day_of_week', y='trip_count',
              title='Taxi Trip Count by Day of Week (All Months)',
              labels={'day_of_week': 'Day of Week', 'trip_count': 'Number of Trips'},
              color='trip_count', color_continuous_scale='Viridis')
fig2.write_html(os.path.join(base_dir, "weekly_pattern_usage.html"))

# 3. Heatmap
df_rev = df_clean.withColumn("day_of_week", date_format("tpep_pickup_datetime", "E"))
avg_revenue_df = df_rev.groupBy("pickup_hour", "day_of_week") \
    .agg(avg("fare_amount").alias("avg_revenue")).toPandas()
avg_revenue_df['day_of_week'] = pd.Categorical(avg_revenue_df['day_of_week'], categories=days_order, ordered=True)
heatmap_data_avg = avg_revenue_df.pivot(index='day_of_week', columns='pickup_hour', values='avg_revenue')
fig3 = go.Figure(data=go.Heatmap(
    z=heatmap_data_avg.values,
    x=heatmap_data_avg.columns,       
    y=heatmap_data_avg.index,          
    colorscale='YlGnBu',
    hoverongaps=False,
    colorbar=dict(title='Avg Fare ($)')
))
fig3.update_layout(
    title="Heatmap of Average Fare by Hour and Day (All Months)",
    xaxis_title="Hour of Day",
    yaxis_title="Day of Week"
)
fig3.write_html(os.path.join(base_dir, "avgfare_hour_day.html"))

# 4. Tip Distribution
payment_types = {
    1: "Credit card", 2: "Cash", 3: "No charge",
    4: "Dispute", 5: "Unknown", 6: "Voided trip"
}
tip_pay_df = df_clean.select("payment_type", "tip_amount").sample(fraction=0.01).toPandas()
tip_pay_df['payment_method'] = tip_pay_df['payment_type'].map(payment_types)
fig4 = px.box(
    tip_pay_df,
    x="payment_method",
    y="tip_amount",
    title="Tip Amount Distribution by Payment Type (All Months)",
    labels={'payment_method': 'Payment Type', 'tip_amount': 'Tip Amount ($)'},
    color="payment_method"
)
fig4.write_html(os.path.join(base_dir, "tip_dsit_meths.html"))

# 5. Hourly Speed
df_duration = df_clean.withColumn("trip_duration_min",
                 (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60)
df_speed = df_duration.withColumn("trip_duration_hr", col("trip_duration_min")/60)
df_speed = df_speed.withColumn("trip_distance_km", col("trip_distance") * 1.60934)
df_speed = df_speed.withColumn("avg_speed_kmph", col("trip_distance_km") / col("trip_duration_hr"))
speed_hour_df = df_speed.groupBy("pickup_hour").agg(
    avg("avg_speed_kmph").alias("avg_speed_kmph")
).orderBy("pickup_hour").toPandas()
fig5 = px.line(speed_hour_df, x="pickup_hour", y="avg_speed_kmph",
              title="Average Taxi Speed by Hour of Day (All Months)", markers=True)
fig5.write_html(os.path.join(base_dir, "hourly_avg_speed.html"))

# 6. Folium Map
print("🗺️ Generating pickup/dropoff map...")
zones = gpd.read_file("./data/zones.geojson")
zones_proj = zones.to_crs(epsg=2263)
zones_proj['centroid'] = zones_proj.geometry.centroid
centroids = zones_proj.set_geometry('centroid').to_crs(epsg=4326)
zones['centroid_lat'] = centroids.geometry.y
zones['centroid_lon'] = centroids.geometry.x
zone_centroids = zones[['location_id', 'centroid_lat', 'centroid_lon']].copy()
zone_centroids['location_id'] = zone_centroids['location_id'].astype(int)

df_sample = df_clean.limit(10000).toPandas()
df_sample['PULocationID'] = df_sample['PULocationID'].astype(int)
df_sample['DOLocationID'] = df_sample['DOLocationID'].astype(int)

df_sample = df_sample.merge(zone_centroids, left_on='PULocationID', right_on='location_id', how='left') \
    .rename(columns={'centroid_lat': 'pickup_lat', 'centroid_lon': 'pickup_lon'}).drop(columns=['location_id'])
zone_centroids_drop = zone_centroids.rename(columns={'centroid_lat': 'dropoff_lat', 'centroid_lon': 'dropoff_lon', 'location_id': 'location_id_dropoff'})
df_sample = df_sample.merge(zone_centroids_drop, left_on='DOLocationID', right_on='location_id_dropoff', how='left').drop(columns=['location_id_dropoff'])

m = folium.Map(location=[40.75, -73.97], zoom_start=11, tiles='CartoDB Dark_Matter')
pickup_cluster = MarkerCluster(name='Pickup Locations').add_to(m)
dropoff_cluster = MarkerCluster(name='Dropoff Locations').add_to(m)

for _, row in df_sample.dropna(subset=['pickup_lat', 'pickup_lon']).iterrows():
    folium.CircleMarker(
        location=[row['pickup_lat'], row['pickup_lon']],
        radius=3, color='blue', fill=True, fill_opacity=0.6,
        popup=f"Pickup ID: {row['PULocationID']}"
    ).add_to(pickup_cluster)

for _, row in df_sample.dropna(subset=['dropoff_lat', 'dropoff_lon']).iterrows():
    folium.CircleMarker(
        location=[row['dropoff_lat'], row['dropoff_lon']],
        radius=3, color='red', fill=True, fill_opacity=0.6,
        popup=f"Dropoff ID: {row['DOLocationID']}"
    ).add_to(dropoff_cluster)

folium.LayerControl().add_to(m)
m.save(os.path.join(base_dir, "pickup_dropoff_map.html"))

# 7. Summary Stats
print("📊 Calculating summary stats...")
total_trips = df_clean.count()
avg_fare = df_clean.selectExpr("avg(fare_amount) as avg_fare").collect()[0]['avg_fare']
avg_fare = builtins.round(avg_fare, 2) if avg_fare is not None else None
total_tips = df_clean.selectExpr("sum(tip_amount) as total_tips").collect()[0]['total_tips']
total_tips = builtins.round(total_tips, 2) if total_tips is not None else None
avg_speed = speed_hour_df['avg_speed_kmph'].mean()
avg_speed = builtins.round(avg_speed, 2) if avg_speed is not None else None

summary_stats = {
    "total_trips": total_trips,
    "avg_fare": avg_fare,
    "total_tips": total_tips,
    "avg_speed_kmph": avg_speed
}

with open(os.path.join(base_dir, "summary.json"), "w") as f:
    json.dump(summary_stats, f, indent=4)

print(f"✅ Combined summary saved to: {os.path.join(base_dir, 'summary.json')}")
spark.stop()



subprocess.run(["python", train_recommender.py])