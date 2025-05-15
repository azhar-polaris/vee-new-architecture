import sys
import os
from fastapi import FastAPI # type: ignore
# Add the `src/` directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))

from router.validation_router import router as validation_router
from router.kafka_router import router as kafka_router


app = FastAPI(title="Validation Service")


@app.get("/health-check")
def root():
    return {"message": "Validation service is running!"}

app.include_router(validation_router)
app.include_router(kafka_router)