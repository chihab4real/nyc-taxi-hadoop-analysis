from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, date_format, col
import pandas as pd
import joblib

def main():
    # List of your 12 parquet files
    parquet_files = [
        "yellow_tripdata_2024-01.parquet",
        "yellow_tripdata_2024-02.parquet",
        "yellow_tripdata_2024-03.parquet",
        "yellow_tripdata_2024-04.parquet",
        "yellow_tripdata_2024-05.parquet",
        "yellow_tripdata_2024-06.parquet",
        "yellow_tripdata_2024-07.parquet",
        "yellow_tripdata_2024-08.parquet",
        "yellow_tripdata_2024-09.parquet",
        "yellow_tripdata_2024-10.parquet",
        "yellow_tripdata_2024-11.parquet",
        "yellow_tripdata_2024-12.parquet",
    ]

    spark = SparkSession.builder.appName("NYC Taxi Pickup Time Recommender Training").getOrCreate()

    # Load all data
    df = spark.read.parquet(*[f"hdfs://namenode:8020/nyc_taxi/{f}" for f in parquet_files])

    df = df.dropna(subset=["tpep_pickup_datetime"])

    # Extract day of week and hour
    df = df.withColumn("day_of_week", date_format(col("tpep_pickup_datetime"), "E"))
    df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))

    # Aggregate counts by day_of_week and hour
    counts_df = df.groupBy("day_of_week", "pickup_hour").count()

    # Convert to pandas
    counts_pd = counts_df.toPandas()

    # Order days
    weekday_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    counts_pd["day_of_week"] = pd.Categorical(counts_pd["day_of_week"], categories=weekday_order, ordered=True)

    # Build recommendations per weekday: best hours (top 25%) and worst hours (bottom 25%)
    recommendations = {}

    for day in weekday_order:
        subset = counts_pd[counts_pd["day_of_week"] == day]
        if subset.empty:
            recommendations[day] = {"best_hours": [], "worst_hours": []}
            continue

        q25 = subset["count"].quantile(0.25)
        q75 = subset["count"].quantile(0.75)

        worst_hours = subset[subset["count"] <= q25]["pickup_hour"].tolist()
        best_hours = subset[subset["count"] >= q75]["pickup_hour"].tolist()

        recommendations[day] = {
            "best_hours": sorted(best_hours),
            "worst_hours": sorted(worst_hours),
        }

    # Save to pickle
    model_path = "pickup_time_recommendation_by_day.pkl"
    joblib.dump(recommendations, model_path)
    print(f"Saved recommendation model to {model_path}")

    spark.stop()

if __name__ == "__main__":
    main()
