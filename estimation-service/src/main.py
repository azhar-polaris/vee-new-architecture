import sys
import os

# Add the `src/` directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from fastapi import FastAPI
from routes.estimation_router import router as estimation_router

app = FastAPI(title="Estimation Service")


@app.get("/health-check")
def root():
    return {"message": "Estimation service is running!"}

app.include_router(estimation_router)