from fastapi import APIRouter
from common.models import SharedModel


router = APIRouter(prefix="/validate", tags=["Validation"])

@router.get("/get_common_data", response_model=SharedModel)
def get_common_data():
    return SharedModel(id=123, name="Validated!")


@router.get("/perform_test_validation")
def perform_test_validation():
    return { "test": "successfull" }

@router.get("/test_kafka")
def test_kafka():
    return { "test": "successfull" }