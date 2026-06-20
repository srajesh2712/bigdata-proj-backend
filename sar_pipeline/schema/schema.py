from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import MetaData
# Define the schema globally
metadata_obj = MetaData(schema="sar")
Base = declarative_base(metadata=metadata_obj)
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, BigInteger
from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import MetaData
metadata_obj = MetaData(schema="sar")
Base = declarative_base(metadata=metadata_obj)

class SafeFile(Base):
    __tablename__ = 'sar_scene_master' # Synced naming with Spark source

    scene_id = Column(Integer, primary_key=True)
    local_path = Column(Text, nullable=False)
    scene_name = Column(String, nullable=False)
    status = Column(String, default='pending')


class ProcessingArtifact(Base):
    __tablename__ = 'processing_artifacts'

    id = Column(Integer, primary_key=True)
    job_id = Column(BigInteger)
    task_id = Column(BigInteger)
    scene_id = Column(BigInteger)
    artifact_type = Column(String)
    file_format = Column(String)
    hdfs_path = Column(String)
    local_path = Column(String)
    file_size_bytes = Column(BigInteger)
    start_time = Column(DateTime)
    stop_time = Column(DateTime)
    duration_seconds = Column(Integer)
    region_wkt = Column(Text)