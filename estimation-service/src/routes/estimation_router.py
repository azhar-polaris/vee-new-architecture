from fastapi import APIRouter
from common.models import SharedModel

router = APIRouter(prefix="/validate", tags=["Validation"])

@router.get("/get_common_data", response_model=SharedModel)
def get_common_data():
    return SharedModel(id=123, name="Estimated!")


@router.get("/perform_test_estimation")
def perform_test_estimation():
    return { "test": "successfull" }