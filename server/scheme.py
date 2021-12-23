from sqlalchemy import Integer, Column, String, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DiagnosisKey(Base):
    __tablename__ = 'diagnosis_keys'

    primary_key = Column(String, primary_key=True)
    region = Column(String)
    sub_region = Column(String)
    key = Column(String(length=24), nullable=False)
    rollingStartNumber = Column(Integer)
    rollingPeriod = Column(Integer)
    reportType = Column(Integer)
    transmissionRisk = Column(Integer)
    daysSinceOnsetOfSymptoms = Column(Integer)
    createdAt = Column(Integer)
    exported = Column(Boolean, default=False)

    def to_serializable_object(self):
        return {
            'keyData': self.key,
            'rollingStartNumber': self.rollingStartNumber,
            'rollingPeriod': self.rollingPeriod,
            'reportType': self.reportType,
            'transmissionRisk': self.transmissionRisk,
            'daysSinceOnsetOfSymptoms': self.daysSinceOnsetOfSymptoms,
            'createdAt': self.createdAt,
        }
