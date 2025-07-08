import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh
from urllib.parse import quote_plus
import requests

# =======================
# 🔧 PostgreSQL Config
# =======================
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "user": "postgres",
    "password": "Predator@123",
    "database": "analytics"
}

password = quote_plus(DB_CONFIG['password'])
engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# =======================
# ⚙️ Streamlit Setup
# =======================
st.set_page_config(page_title="Realtime User Analytics", layout="wide")
st_autorefresh(interval=10 * 1000, key="refresh_dashboard")

st.title("🛍️ Real-Time E-commerce Event Dashboard")

# =======================
# 🔁 Load Data Functions
# =======================
@st.cache_data(ttl=10)
def load_funnel():
    return pd.read_sql("SELECT * FROM funnel_summary ORDER BY window_end DESC LIMIT 100", engine)

@st.cache_data(ttl=10)
def load_trends():
    return pd.read_sql("SELECT * FROM event_trends ORDER BY window_end DESC LIMIT 100", engine)

@st.cache_data(ttl=10)
def load_churn_predictions():
    return pd.read_sql(
        "SELECT * FROM churn_predictions ORDER BY window_end DESC LIMIT 100",
        con=engine
    )


def predict_churn(features):
    try:
        res = requests.post("http://127.0.0.1:8000/predict_churn", json=features)
        if res.status_code == 200:
            data = res.json()
            return {
                "churn_risk": data.get("churn_risk", 0),
                "churn_probability": data.get("churn_probability", 0.0)
            }
        else:
            return {"churn_risk": 0, "churn_probability": 0.0}
    except Exception as e:
        print("Prediction error:", e)
        return {"churn_risk": 0, "churn_probability": 0.0}



# =======================
# 🧠 Churn Scoring Logic
# =======================
def compute_churn(df):
    churn_rows = []

    df["event_type"] = df["event_type"].str.lower().str.strip()
    agg_df = df.groupby(["window_start", "window_end", "event_type"], as_index=False)["unique_users"].sum()

    pivot = agg_df.pivot(
        index=["window_start", "window_end"],
        columns="event_type",
        values="unique_users"
    ).fillna(0).reset_index()

    pivot = pivot.rename(columns={
        "click": "num_views",
        "add_to_cart": "num_cart_adds"
    })

    pivot["session_duration"] = (pivot["window_end"] - pivot["window_start"]).dt.total_seconds()

    for _, row in pivot.iterrows():
        features = {
            "num_views": int(row.get("num_views", 0)),
            "num_cart_adds": int(row.get("num_cart_adds", 0)),
            "session_duration": row["session_duration"]
        }

       

        prediction = predict_churn(features)
        # Defensive check: skip if keys missing
        if "churn_risk" not in prediction or "churn_probability" not in prediction:
            continue

        churn_rows.append({
            **features,
            "window_start": row["window_start"],
            "window_end": row["window_end"],
            "churn_risk": prediction["churn_risk"],
            "churn_probability": prediction["churn_probability"]
        })

    return pd.DataFrame(churn_rows)


def save_churn_to_db(churn_df):
    if not churn_df.empty:
        churn_df["batch_time"] = pd.Timestamp.now()
        try:
            churn_df.to_sql("churn_predictions", con=engine, if_exists="append", index=False)
            st.success("Churn predictions saved to database.")
        except Exception as e:
            st.error(f"❌ Failed to write churn data: {e}")
    else:
        st.warning("⚠️ No churn predictions to save.")

# =======================
# 📊 Dashboard Layout
# =======================
col1, col2 = st.columns(2)

with col1:
    st.subheader("🔁 Funnel Summary")
    funnel_df = load_funnel()
    st.dataframe(funnel_df, use_container_width=True)

with col2:
    st.subheader("📈 Event Trends (KPIs)")
    trend_df = load_trends()
    st.dataframe(trend_df, use_container_width=True)


# =======================
st.subheader("📊 Churn Risk Trends")

churn_data = load_churn_predictions()

if churn_data.empty:
    st.info("No churn prediction data available.")
else:
    # Convert time columns to datetime (in case they aren’t)
    churn_data["window_start"] = pd.to_datetime(churn_data["window_start"])
    churn_data["window_end"] = pd.to_datetime(churn_data["window_end"])

    # Line chart of churn probability over time
    st.line_chart(
        churn_data.sort_values("window_end")[["window_end", "churn_probability"]].set_index("window_end"),
        use_container_width=True
    )

   

    # Optional: Show raw data
    with st.expander("📋 Show Prediction Data"):
        st.dataframe(churn_data, use_container_width=True)



# =======================
# 🔮 Churn Prediction Section
# =======================
st.markdown("---")
st.subheader(" Churn Risk Predictions (Live)")

churn_df = compute_churn(funnel_df)



if churn_df.empty:
    st.info("No recent user sessions to score.")
else:
    st.dataframe(
        churn_df.sort_values("window_end", ascending=False),
        use_container_width=True
    )

# ✅ Save to DB
save_churn_to_db(churn_df)

st.caption("🔁 Auto-refreshing every 10 seconds...")
