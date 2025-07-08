import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib

# Step 1: Simulate synthetic session data
np.random.seed(42)
data = []

for _ in range(1000):
    views = np.random.poisson(lam=20)
    cart_adds = np.random.binomial(n=views, p=np.random.uniform(0.05, 0.7))  # conversion ratio
    session_duration = np.random.randint(30, 600)  # 30s to 10min

    # Step 2: Apply churn label logic
    if views < 10 or (cart_adds / (views + 1)) < 0.2:
        label = 1  # churn
    else:
        label = 0  # not churn

    data.append({
        "num_views": views,
        "num_cart_adds": cart_adds,
        "session_duration": session_duration,
        "churn": label
    })

df = pd.DataFrame(data)

# Step 3: Train/test split
X = df[["num_views", "num_cart_adds", "session_duration"]]
y = df["churn"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Step 4: Train model
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Step 5: Evaluate model
print("\n✅ Classification Report:")
print(classification_report(y_test, model.predict(X_test)))

# Step 6: Save model
joblib.dump(model, "ml_model/churn_model.pkl")
print("\n✅ New churn_model.pkl saved successfully!")
