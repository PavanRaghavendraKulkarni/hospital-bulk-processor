from dataclasses import dataclass

from fastapi import APIRouter, Depends, File, HTTPException, Query, Response, UploadFile, status
from fastapi.responses import StreamingResponse

from app.config import Settings, get_settings
from app.dependencies import get_bulk_service
from app.models import JobStatus
from app.schemas import (
    BulkRetryResponse,
    BulkStatusResponse,
    BulkUploadResponse,
    CsvValidationResponse,
    ErrorResponse,
)
from app.services.bulk_service import BulkService

router = APIRouter(prefix="/hospitals/bulk", tags=["bulk"])

VALIDATION_RESPONSES = {
    400: {"model": ErrorResponse, "description": "File-level problem (type, size, encoding, header, row limit)"},
    422: {"model": ErrorResponse, "description": "One or more rows are invalid"},
}


@dataclass(frozen=True)
class Upload:
    content: bytes
    filename: str | None


async def read_upload(
    file: UploadFile = File(..., description="CSV with header name,address,phone"),
    settings: Settings = Depends(get_settings),
) -> Upload:
    # Read at most one byte past the limit: enough to detect oversize without buffering it all.
    return Upload(await file.read(settings.max_upload_bytes + 1), file.filename)


def status_url(batch_id: str) -> str:
    return f"/hospitals/bulk/{batch_id}/status"


@router.post(
    "",
    response_model=BulkStatusResponse | BulkUploadResponse,
    responses={
        200: {"model": BulkStatusResponse, "description": "wait=true and the job finished: full processing result"},
        202: {"model": BulkUploadResponse, "description": "wait=false, or the sync wait timed out: poll status_url"},
        **VALIDATION_RESPONSES,
        503: {"model": ErrorResponse},
    },
)
async def upload_bulk(
    response: Response,
    wait: bool = Query(True, description="Wait for the job to finish and return its result (falls back to 202)"),
    upload: Upload = Depends(read_upload),
    service: BulkService = Depends(get_bulk_service),
) -> BulkStatusResponse | BulkUploadResponse:
    submission = await service.submit(upload.content, upload.filename)
    if wait and (result := await service.wait_for_result(submission.batch_id)) is not None:
        return result
    response.status_code = status.HTTP_202_ACCEPTED
    return BulkUploadResponse(**submission.model_dump(), status_url=status_url(submission.batch_id))


@router.get("/{batch_id}/status", response_model=BulkStatusResponse, responses={404: {"model": ErrorResponse}})
async def get_bulk_status(batch_id: str, service: BulkService = Depends(get_bulk_service)) -> BulkStatusResponse:
    job_status = await service.get_status(batch_id)
    if job_status is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Batch {batch_id} not found")
    return job_status


@router.post("/validate", response_model=CsvValidationResponse, responses=VALIDATION_RESPONSES)
async def validate_bulk(
    upload: Upload = Depends(read_upload), service: BulkService = Depends(get_bulk_service)
) -> CsvValidationResponse:
    rows = service.validate(upload.content, upload.filename)
    return CsvValidationResponse(valid=True, total_rows=len(rows), rows=rows)


@router.post(
    "/{batch_id}/retry",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BulkRetryResponse,
    responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def retry_bulk(batch_id: str, service: BulkService = Depends(get_bulk_service)) -> BulkRetryResponse:
    retried = await service.retry(batch_id)
    if retried is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Batch {batch_id} not found")
    return BulkRetryResponse(
        batch_id=batch_id, status=JobStatus.QUEUED, retried_rows=retried, status_url=status_url(batch_id)
    )


@router.get(
    "/{batch_id}/events",
    response_class=StreamingResponse,
    responses={200: {"content": {"text/event-stream": {}}}, 404: {"model": ErrorResponse}},
)
async def stream_bulk_events(batch_id: str, service: BulkService = Depends(get_bulk_service)) -> StreamingResponse:
    if await service.get_status(batch_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Batch {batch_id} not found")
    return StreamingResponse(
        service.stream_status(batch_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
