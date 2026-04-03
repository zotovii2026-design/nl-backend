from sqlalchemy import Column, String, DateTime, Integer, Text, func
from sqlalchemy.dialects.postgresql import UUID
from core.database import Base
import uuid


class SyncLog(Base):
    __tablename__ = "sync_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    task_name = Column(String(100), nullable=False)  # sync_wb_products, sync_wb_sales, etc.
    status = Column(String(50), nullable=False)  # pending, running, completed, failed
    synced_count = Column(Integer, nullable=True)  # количество синхронизированных записей
    error_message = Column(Text, nullable=True)  # текст ошибки, если статус failed
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Integer, nullable=True)  # длительность выполнения
    created_at = Column(DateTime(timezone=True), server_default=func.now())
