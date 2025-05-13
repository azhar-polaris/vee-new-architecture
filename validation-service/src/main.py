import sys
import os

# Add the `src/` directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from fastapi import FastAPI
from routes.validation_router import router as validation_router

app = FastAPI(title="Validation Service")


@app.get("/health-check")
def root():
    return {"message": "Validation service is running!"}

app.include_router(validation_router)