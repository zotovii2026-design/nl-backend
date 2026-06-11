import uuid

from sqlalchemy import Column, DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID

from core.database import Base


class CeleryTaskRun(Base):
    __tablename__ = "celery_task_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(String(64), nullable=False, unique=True)
    task_name = Column(String(150), nullable=False, index=True)
    status = Column(String(20), nullable=False, index=True)
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Integer, nullable=True)
    result_summary = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)


__all__ = ["CeleryTaskRun"]
