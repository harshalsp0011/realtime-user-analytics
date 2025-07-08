# churn_predictor.py

from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import numpy as np
import uvicorn

# Load model
model = joblib.load("ml_model/churn_model.pkl")

# Define request schema
class SessionFeatures(BaseModel):
    num_views: int
    num_cart_adds: int
    session_duration: float  # in seconds

class ChurnInput(BaseModel):
    num_views: int
    num_cart_adds: int
    session_duration: int

# Initialize app
app = FastAPI(
    title="Churn Prediction API",
    description="Predict whether a user session is likely to churn (no purchase)",
    version="1.0"
)



@app.post("/predict_churn")
def predict_churn(input: ChurnInput):
    input_data = np.array([[input.num_views, input.num_cart_adds, input.session_duration]])
    proba = float(model.predict_proba(input_data)[0][1])  # churn = label 1

    if proba >= 0.8:
        risk = "High"
    elif proba >= 0.5:
        risk = "Medium"
    else:
        risk = "Low"

    return {
        "churn_probability": round(proba, 2),
        "churn_risk_level": risk
    }


# Local dev runner
if __name__ == "__main__":
    uvicorn.run("ml_model.churn_predictor:app", host="0.0.0.0", port=8000, reload=True)
