import sys
import os
import threading
from fastapi import FastAPI # type: ignore

# Add the `src/` directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))

from router.estimation_router import router as estimation_router
from consumers import start_profile_instant_consumer

app = FastAPI(title="Estimation Service")

app.state.cancel_event = threading.Event()
app.state.consumer_threads = []


@app.on_event("startup")
def startup_kafka_consumers():
    app.state.consumer_threads.append(start_profile_instant_consumer(app.state.cancel_event))

@app.on_event("shutdown")
def shutdown_kafka_consumers():
    app.state.cancel_event.set()
    for thread in app.state.consumer_threads:
        thread.join()

@app.get("/health-check")
def root():
    return {"message": "Estimation service is running!"}

app.include_router(estimation_router)