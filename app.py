import streamlit as st
import streamlit.components.v1 as components
import os
import re
from datetime import datetime
import json

st.set_page_config(page_title="NYC Taxi Analysis", layout="wide")
st.title("NYC Taxi Data Analysis 2024")

# Helper: Convert folder name to readable month label
def extract_month_label(folder_name):
    if folder_name == "all":
        return "Whole Year"
    match = re.search(r"(\d{4})-(\d{2})", folder_name)
    if match:
        year, month = match.groups()
        dt = datetime.strptime(f"{year}-{month}", "%Y-%m")
        return dt.strftime("%b %Y")  # e.g. "Jan 2024"
    return folder_name

# Find all saved plot folders
plot_base_dir = "./saved_plots"
all_plot_folders = sorted([
    f for f in os.listdir(plot_base_dir)
    if os.path.isdir(os.path.join(plot_base_dir, f)) and (
        re.match(r".*_\d{4}-\d{2}$", f) or f == "all"
    )
], reverse=True)

# Map folder to label

ordered_labels = ["Whole Year"] + [datetime(2024, m, 1).strftime("%b %Y") for m in range(1, 13)]

# Build folder-label map
folder_label_map = {extract_month_label(f): f for f in all_plot_folders}

# Only keep options that actually exist
month_options = [label for label in ordered_labels if label in folder_label_map]


# Initialize session state
if "selected_month" not in st.session_state:
    st.session_state.selected_month = month_options[0]

# Plot HTML loader
def load_plot_html(plots_folder, filename, height=500):
    full_path = os.path.join(plots_folder, filename)
    if not os.path.exists(full_path):
        st.error(f"Plot not found: {full_path}")
        return
    with open(full_path, "r") as f:
        html_data = f.read()
    components.html(html_data, height=height, scrolling=True)

# Month Selector UI
st.markdown("### Select Month or Whole Year")
cols = st.columns(len(month_options))

for i, month_label in enumerate(month_options):
    button_style = """
        <style>
        .month-button {
            background-color: #f0f2f6;
            border-radius: 8px;
            padding: 10px;
            margin: 4px;
            text-align: center;
            font-weight: bold;
            cursor: pointer;
            user-select: none;
        }
        .month-button:hover {
            background-color: #e0e7ff;
        }
        .selected {
            background-color: #6366f1;
            color: white !important;
        }
        </style>
    """
    if i == 0:
        st.markdown(button_style, unsafe_allow_html=True)

    selected_class = "selected" if st.session_state.selected_month == month_label else ""

    if cols[i].button(f"{month_label}", key=month_label):
        st.session_state.selected_month = month_label

# Determine selected folder
selected_folder = folder_label_map[st.session_state.selected_month]
plots_folder = os.path.join(plot_base_dir, selected_folder)

# Summary stats
summary_path = os.path.join(plots_folder, "summary.json")
summary_stats = None

if os.path.exists(summary_path):
    with open(summary_path, "r") as f:
        summary_stats = json.load(f)

if summary_stats:
    st.markdown("### 📈 Key Statistics")
    card1, card2, card3, card4 = st.columns(4)

    card1.metric("Total Trips", f"{summary_stats['total_trips']:,}")
    card2.metric("Avg Fare ($)", f"{summary_stats['avg_fare']:.2f}" if summary_stats['avg_fare'] is not None else "N/A")
    card3.metric("Total Tips ($)", f"{summary_stats['total_tips']:.2f}" if summary_stats['total_tips'] is not None else "N/A")
    card4.metric("Avg Speed (km/h)", f"{summary_stats['avg_speed_kmph']:.2f}" if summary_stats['avg_speed_kmph'] is not None else "N/A")
else:
    st.warning("Summary statistics not found for this month.")

# Plot display
with st.spinner(f"Loading plots for {st.session_state.selected_month}..."):
    tabs = st.tabs([
        "Hourly Distribution of Taxi Trips", 
        "Weekly Patterns in Taxi Usage", 
        "Heatmap of Average Fare by Hour and Day", 
        "Tip Distribution Across Payment Methods",
        "Hourly Average Speed of Taxis",
        "Pickup & Dropoff Map"
    ])

    with tabs[0]:
        st.subheader("Hourly Distribution of Taxi Trips")
        load_plot_html(plots_folder, "hourly_dist.html", height=500)

    with tabs[1]:
        st.subheader("Weekly Patterns in Taxi Usage")
        load_plot_html(plots_folder, "weekly_pattern_usage.html", height=500)

    with tabs[2]:
        st.subheader("Heatmap of Average Fare by Hour and Day")
        load_plot_html(plots_folder, "avgfare_hour_day.html", height=500)

    with tabs[3]:
        st.subheader("Tip Distribution Across Payment Methods")
        load_plot_html(plots_folder, "tip_dsit_meths.html", height=500)

    with tabs[4]:
        st.subheader("Hourly Average Speed of Taxis")
        load_plot_html(plots_folder, "hourly_avg_speed.html", height=500)

    with tabs[5]:
        st.subheader("Pickup and Dropoff Location Map")
        load_plot_html(plots_folder, "pickup_dropoff_map.html", height=650)
