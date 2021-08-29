from sqlalchemy import Integer, Column, String, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DiagnosisKey(Base):
    __tablename__ = 'diagnosis_keys'

    id = Column(Integer, primary_key=True, autoincrement=True)
    cluster_id = Column(String(length=6))
    key = Column(String(length=24), nullable=False)
    rollingStartNumber = Column(Integer)
    rollingPeriod = Column(Integer)
    reportType = Column(Integer)
    transmissionRisk = Column(Integer)
    daysSinceOnsetOfSymptoms = Column(Integer)
    createdAt = Column(Integer)
    exported = Column(Boolean, default=False)
