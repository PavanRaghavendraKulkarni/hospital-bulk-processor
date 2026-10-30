from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status

from app.config import Settings, get_settings
from app.dependencies import get_bulk_service
from app.schemas import BulkStatusResponse, BulkUploadResponse, ErrorResponse
from app.services.bulk_service import BulkService

router = APIRouter(prefix="/hospitals/bulk", tags=["bulk"])

VALIDATION_RESPONSES = {
    400: {"model": ErrorResponse, "description": "File-level problem (type, size, encoding, header, row limit)"},
    422: {"model": ErrorResponse, "description": "One or more rows are invalid"},
}


def status_url(batch_id: str) -> str:
    return f"/hospitals/bulk/{batch_id}/status"


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BulkUploadResponse,
    responses={**VALIDATION_RESPONSES, 503: {"model": ErrorResponse}},
)
async def upload_bulk(
    file: UploadFile = File(..., description="CSV with header name,address,phone"),
    service: BulkService = Depends(get_bulk_service),
    settings: Settings = Depends(get_settings),
) -> BulkUploadResponse:
    # Read at most one byte past the limit: enough to detect oversize without buffering it all.
    content = await file.read(settings.max_upload_bytes + 1)
    submission = await service.submit(content, file.filename)
    return BulkUploadResponse(**submission.model_dump(), status_url=status_url(submission.batch_id))


@router.get("/{batch_id}/status", response_model=BulkStatusResponse, responses={404: {"model": ErrorResponse}})
async def get_bulk_status(batch_id: str, service: BulkService = Depends(get_bulk_service)) -> BulkStatusResponse:
    job_status = await service.get_status(batch_id)
    if job_status is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Batch {batch_id} not found")
    return job_status
