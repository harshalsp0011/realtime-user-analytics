# churn_model_train.py

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib
import psycopg2
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Connect to PostgreSQL
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

# Step 1: Load funnel_summary data (we simulate sessions here)
df = pd.read_sql("SELECT * FROM funnel_summary", engine)

# Step 2: Pivot to create one row per session (per window)
pivot = df.pivot_table(
    index=["window_start", "window_end"],
    columns="event_type",
    values="unique_users",
    fill_value=0
).reset_index()

# Flatten column names
pivot.columns.name = None

# Safely rename only if they exist
col_map = {}
if 'view' in pivot.columns:
    col_map['view'] = 'num_views'
else:
    pivot['num_views'] = 0

if 'cart' in pivot.columns:
    col_map['cart'] = 'num_cart_adds'
else:
    pivot['num_cart_adds'] = 0

if 'purchase' in pivot.columns:
    col_map['purchase'] = 'num_purchases'
else:
    pivot['num_purchases'] = 0

pivot = pivot.rename(columns=col_map)

# Step 3: Add session duration
pivot['session_duration'] = (pivot['window_end'] - pivot['window_start']).dt.total_seconds()


# Step 4: Define churn label
pivot['churned'] = pivot['num_purchases'].apply(lambda x: 0 if x > 0 else 1)

# Step 5: Define features and labels
features = ['num_views', 'num_cart_adds', 'session_duration']
X = pivot[features]
y = pivot['churned']

# Step 6: Train model
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y_train)

# Step 7: Evaluate
y_pred = clf.predict(X_test)
print(classification_report(y_test, y_pred))

# Step 8: Save model
joblib.dump(clf, "ml_model/churn_model.pkl")
print("✅ Model saved to churn_model.pkl")
