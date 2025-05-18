from sqlalchemy import Column, String, Integer, ForeignKey, Text, DateTime
from sqlalchemy.orm import relationship
from api import Base
from datetime import datetime


class Scenario(Base):
    __tablename__ = "scenarios"
    id = Column(String, primary_key=True, index=True)
    status = Column(String, index=True)
    predictions = relationship("Prediction", back_populates="scenario")
    last_heartbeat = Column(DateTime, default=datetime.utcnow)


class Prediction(Base):
    __tablename__ = "predictions"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    scenario_id = Column(String, ForeignKey("scenarios.id"))
    label = Column(String)
    confidence = Column(String)
    extra = Column(Text)  # Можно хранить JSON в строке, например

    scenario = relationship("Scenario", back_populates="predictions")
