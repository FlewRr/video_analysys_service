from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class Scenario(Base):
    __tablename__ = 'scenarios'
    id = Column(String, primary_key=True)
    state = Column(String)
    updated_at = Column(DateTime, default=datetime.utcnow)

class OutboxEvent(Base):
    __tablename__ = 'outbox_events'
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String)
    payload = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    sent = Column(Boolean, default=False)
    sent_at = Column(DateTime, nullable=True)

engine = create_engine('sqlite:///db')
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)
