from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class SafeFile(Base):
    __tablename__ = 'safe_files'

    id = Column(Integer, primary_key=True)
    folder_path = Column(Text, nullable=False)
    status = Column(String, default='pending')  # 'pending', 'processing', 'done', 'error'
    active = Column(Boolean, default=True)
    inserted_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    action = Column(String)

class Jobs(Base):
    __tablename__ = 'Jobs'

    id = Column(Integer, primary_key=True)
    status = Column(String, default='pending')  # 'pending', 'processing', 'done', 'error'
    active = Column(Boolean, default=True)
    inserted_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    action = Column(String)

