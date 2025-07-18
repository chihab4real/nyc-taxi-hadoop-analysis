# NYC Taxi Data Hadoop Analysis with Dashboard

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange.svg)
![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.0+-yellow.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.29.0-red.svg)
![Docker](https://img.shields.io/badge/Docker-20.0+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## 🚕 Project Overview

This project demonstrates **end-to-end big data analytics** using NYC Taxi trip data. It showcases the complete data pipeline from raw data processing through Apache Hadoop and Spark to interactive visualization dashboards. The project processes millions of taxi trip records to generate insights about transportation patterns, fare analysis, and geographical usage trends.

**Key Highlights:**
- **Big Data Processing**: Handles large-scale taxi trip data using Apache Hadoop HDFS and Apache Spark
- **Interactive Dashboard**: Real-time data exploration through Streamlit web application
- **Geospatial Analysis**: Location-based insights using Folium maps and GeoJSON data
- **Time Series Analytics**: Hourly, daily, and monthly trend analysis
- **Containerized Deployment**: Full Docker-based infrastructure for reproducible deployment

This project provides an interactive dashboard for exploring New York City taxi trip data. It includes various visualizations like hourly trip distribution, weekly trends, average fare heatmaps, tip patterns, and spatial maps of pickups and dropoffs. The app is built using Streamlit and powered by PySpark for efficient large-scale data processing.

## 🚀 Live Demo

> **Note**: Due to the large dataset size and infrastructure requirements, this project runs locally with Docker. Follow the [setup instructions](#how-to-run) below to run the dashboard on your machine.

## Screenshots

### Dashboard Overview
![Dashboard Main View](screenshots/dash_overview.png)

### Hourly Trip Distribution
![Hourly Analysis](screenshots/hourly_dist.png)

### Average Fare by Hour and Day Heatmap
![Fare Heatmap](screenshots/avg_fare.png)

### Pickup/DropOff Map
![Map Analysis](screenshots/geo_map.png)

## 🛠️ Technical Skills Demonstrated

This project showcases proficiency in:

**Big Data Technologies:**
- Apache Hadoop HDFS - Distributed file system management
- Apache Spark (PySpark) - Large-scale data processing and analytics
- Parquet format - Efficient columnar data storage

**Data Science & Analytics:**
- Exploratory Data Analysis (EDA)
- Time series analysis and pattern recognition
- Geospatial data processing and visualization
- Statistical aggregations and business intelligence

**Software Engineering:**
- Containerization with Docker and Docker Compose
- RESTful web application development
- Code organization and project structure
- Version control and collaborative development

**Data Visualization:**
- Interactive dashboards with Streamlit
- Advanced plotting with Plotly
- Geospatial mapping with Folium
- Real-time data visualization

## Technologies Used

### Big Data & Processing
- **Apache Hadoop** - Distributed storage and processing framework
- **Apache Spark (PySpark)** - Large-scale data processing and analytics
- **HDFS** - Hadoop Distributed File System for data storage

### Development & Analytics
- **Python 3.8+** - Primary programming language
- **Jupyter Notebook** - Interactive development and analysis
- **Pandas** - Data manipulation and analysis
- **NumPy** - Numerical computing

### Visualization & UI
- **Streamlit** - Web application framework for the dashboard
- **Plotly** - Interactive plotting and visualization
- **Folium** - Interactive maps and geospatial visualization
- **GeoPandas** - Geospatial data processing

### Infrastructure & Deployment
- **Docker** - Containerization platform
- **Docker Compose** - Multi-container application orchestration

### Data Format & Storage
- **Apache Parquet** - Columnar storage format for efficient querying
- **GeoJSON** - Geographic data interchange format

## Database Schema (Parquet Data)

The NYC Taxi data contains columns such as:
- `tpep_pickup_datetime` (timestamp): Pickup time
- `tpep_dropoff_datetime` (timestamp): Dropoff time
- `passenger_count` (int): Number of passengers
- `trip_distance` (float): Distance of trip in miles
- `fare_amount` (float): Fare in USD
- `tip_amount` (float): Tip in USD
- `payment_type` (int): Payment method code
- `PULocationID` (int): Pickup location zone ID
- `DOLocationID` (int): Dropoff location zone ID

Additional fields may be present depending on the dataset version.

## ✨ Features

### 📊 **Data Analytics Dashboard**
- **Temporal Analysis**: Hourly and weekly usage patterns with interactive visualizations
- **Financial Insights**: Fare and tip analysis across different payment methods
- **Geospatial Mapping**: Interactive maps showing pickup/dropoff locations with clustering
- **Performance Metrics**: Speed analysis and trip efficiency calculations

### 🔍 **Advanced Analytics**
- **Time-based Filtering**: Monthly or full-year data selection for comparative analysis
- **Statistical Summary**: Comprehensive trip statistics (count, average fare, speed, tips)
- **Pattern Recognition**: Peak hours identification and demand forecasting
- **Recommendation System**: Optimal pickup time suggestions based on historical data

### 🎯 **Interactive Features**
- **Real-time Dashboard**: Responsive Streamlit interface with multiple visualization tabs
- **Data Filtering**: Dynamic month selection and data subset analysis
- **Export Capabilities**: Pre-computed visualizations stored as HTML files
- **Scalable Architecture**: Designed to handle large datasets efficiently

## Challenges Encountered

- **HDFS setup and file management:** Uploading parquet files to HDFS inside Docker required manual setup of directories and permissions.
- **PySpark environment:** Ensuring PySpark and related dependencies worked correctly within the container environment.
- **Coordinate transformations:** Mapping zone IDs to geographic coordinates required handling projections and merging GeoJSON data with trip data.
- **Performance:** Processing large datasets efficiently with Spark and sampling for Folium maps to keep them responsive.
- **Interactive visualizations:** Integrating Plotly and Folium with Streamlit required saving plots as HTML and embedding them correctly.

## Team Members

* [Chihabeddine Zitouni](https://github.com/chihab4real)
* [Patrick Molina](https://github.com/patrickmolina1/)

## How to Run

### 1. Clone the Repository

```bash
git clone https://github.com/chihab4real/nyc-taxi-hadoop-analysis
cd nyc-taxi-hadoop-analysis
```

### 2. Download Data

Get the yellow taxi trip data (in .parquet format) from [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and place it into the `shared/data/` folder.

### 3. Start the Hadoop + Spark Cluster

```bash
docker-compose up -d
```

### 4. Move Data to HDFS

Enter the namenode container:
```bash
docker exec -it namenode bash
```

Inside the container:
```bash
hdfs dfs -mkdir -p /nyc_taxi
hdfs dfs -put /shared/data/*.parquet /nyc_taxi/
```

### 5. Open Jupyter Notebook

Open your browser and go to http://localhost:8888

### 6. Precompute Visualizations

After moving the data files to HDFS, run the visualization precomputation in the Jupyter notebook terminal:
```bash
python generate_plots.py
```

### 7. Launch the Streamlit App

Run the following command:
```bash
streamlit run app.py
```

Then open in your browser: http://localhost:8501/

## 📁 Folder Structure

```
nyc-taxi-hadoop-analysis/
├── app.py                  # Streamlit dashboard application
├── requirements.txt        # Python dependencies
├── docker-compose.yml      # Docker orchestration configuration
├── generate_plots.py       # Visualization generator for all months
├── get_plots_month.py      # Spark-based preprocessing for single month
├── train_recommender.py    # Machine learning model for recommendations
├── config/                 # Hadoop configuration files
├── data/                   # Static data files (zones, lookups)
│   ├── taxi_zone_lookup.csv
│   ├── taxi_zones.csv
│   └── zones.geojson
├── notebooks/              # Jupyter notebooks for analysis
│   └── analysis.ipynb      
├── saved_plots/            # Pre-computed visualizations
│   ├── all/                # Combined yearly analysis
│   └── yellow_tripdata_2024-XX/  # Monthly analysis folders
├── screenshots/            # Dashboard screenshots
├── shared/                 # Docker volume for data sharing
│   └── data/              # Raw parquet files
└── README.md              # Project documentation
```

## Requirements

Install Python dependencies:
```bash
pip install -r requirements.txt
```

Or manually install individual packages:
```bash
pip install streamlit pandas geopandas pyspark plotly folium numpy joblib
```

## System Requirements

### Hardware Requirements
- **RAM**: Minimum 8GB, recommended 16GB+ for large datasets
- **Storage**: At least 10GB free space for data and Docker images
- **CPU**: Multi-core processor recommended for Spark processing

### Software Requirements
- **Docker** (version 20.0+)
- **Docker Compose** (version 1.27+)
- **Python** 3.8 or higher
- **Git** for repository management

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- At least 8GB RAM recommended for processing large datasets

## Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Raw Data      │    │   Hadoop/Spark   │    │   Streamlit     │
│   (.parquet)    │───▶│     Cluster      │───▶│   Dashboard     │
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   NYC TLC       │    │   PySpark        │    │   Interactive   │
│   Trip Records  │    │   Analytics      │    │   Visualizations│
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Data Sources

- [NYC Taxi & Limousine Commission Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- NYC Taxi Zone Shapefiles for geographic mapping

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📈 Future Enhancements

- **Real-time Data Integration**: Connect to live taxi data streams
- **Machine Learning Models**: Predictive analytics for demand forecasting
- **Advanced Visualizations**: 3D mapping and enhanced geospatial analysis
- **Performance Optimization**: Query optimization and caching strategies
- **Mobile-Responsive Design**: Enhanced mobile experience for the dashboard

## License

This project is open source and available under the [MIT License](LICENSE).