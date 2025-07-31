import uuid
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, String, DateTime, ForeignKey, Text,
    Integer, BigInteger, JSON
)
from sqlalchemy.orm import declarative_base, relationship, Mapped

# Define the declarative base
Base = declarative_base()

# --- User Model ---
class User(Base):
    """
    SQLAlchemy model for the 'users' table.
    Stores user account information.
    """
    __tablename__ = 'users'

    id: Mapped[uuid.UUID] = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = Column(String(255), nullable=False)
    email: Mapped[str] = Column(String(255), nullable=False, unique=True)
    created_at: Mapped[datetime] = Column(DateTime(timezone=True), default=datetime.utcnow)

    # Relationships
    schemas: Mapped[list["Schema"]] = relationship("Schema", back_populates="creator")
    connections: Mapped[list["DatabaseConnection"]] = relationship("DatabaseConnection", back_populates="creator")
    jobs: Mapped[list["Job"]] = relationship("Job", back_populates="creator")

# --- Schema Model ---
class Schema(Base):
    """
    SQLAlchemy model for the 'schemas' table.
    Stores the structure and fields for synthetic data generation.
    """
    __tablename__ = 'schemas'

    id: Mapped[uuid.UUID] = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = Column(String(255), nullable=False)
    description: Mapped[str | None] = Column(Text)
    version: Mapped[str | None] = Column(String(50))
    fields: Mapped[dict] = Column(JSON, nullable=False)
    created_by: Mapped[uuid.UUID | None] = Column(String, ForeignKey('users.id', ondelete='SET NULL'))
    created_at: Mapped[datetime] = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    creator: Mapped["User"] = relationship("User", back_populates="schemas")
    jobs: Mapped[list["Job"]] = relationship("Job", back_populates="schema_definition")

# --- Database Connection Model ---
class DatabaseConnection(Base):
    """
    SQLAlchemy model for the 'database_connections' table.
    Stores connection details for target databases.
    """
    __tablename__ = 'database_connections'

    id: Mapped[uuid.UUID] = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = Column(String(255), nullable=False)
    type: Mapped[str] = Column(String(100), nullable=False)
    status: Mapped[str] = Column(String(50), nullable=False, default='Disconnected')
    details: Mapped[str] = Column(Text, nullable=False) # Should be encrypted in a real app
    created_by: Mapped[uuid.UUID | None] = Column(String, ForeignKey('users.id', ondelete='SET NULL'))
    created_at: Mapped[datetime] = Column(DateTime(timezone=True), default=datetime.utcnow)

    # Relationships
    creator: Mapped["User"] = relationship("User", back_populates="connections")
    jobs: Mapped[list["Job"]] = relationship("Job", back_populates="destination")

# --- Job Model ---
class Job(Base):
    """
    SQLAlchemy model for the 'jobs' table.
    Defines a synthetic data generation job.
    """
    __tablename__ = 'jobs'

    id: Mapped[uuid.UUID] = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = Column(String(255), nullable=False)
    status: Mapped[str] = Column(String(50), nullable=False, default='Draft')
    schema_id: Mapped[uuid.UUID] = Column(String, ForeignKey('schemas.id', ondelete='CASCADE'), nullable=False)
    destination_id: Mapped[uuid.UUID | None] = Column(String, ForeignKey('database_connections.id', ondelete='SET NULL'))
    simulation_rules: Mapped[dict | None] = Column(JSON)
    output_settings: Mapped[dict | None] = Column(JSON)
    created_by: Mapped[uuid.UUID | None] = Column(String, ForeignKey('users.id', ondelete='SET NULL'))
    created_at: Mapped[datetime] = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    creator: Mapped["User"] = relationship("User", back_populates="jobs")
    schema_definition: Mapped["Schema"] = relationship("Schema", back_populates="jobs")
    destination: Mapped["DatabaseConnection"] = relationship("DatabaseConnection", back_populates="jobs")
    runs: Mapped[list["JobRun"]] = relationship("JobRun", back_populates="job", cascade="all, delete-orphan")

# --- Job Run Model ---
class JobRun(Base):
    """
    SQLAlchemy model for the 'job_runs' table.
    Logs the history and metrics of each job execution.
    """
    __tablename__ = 'job_runs'

    id: Mapped[uuid.UUID] = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    job_id: Mapped[uuid.UUID] = Column(String, ForeignKey('jobs.id', ondelete='CASCADE'), nullable=False)
    status: Mapped[str] = Column(String(50), nullable=False)
    started_at: Mapped[datetime] = Column(DateTime(timezone=True), default=datetime.utcnow)
    finished_at: Mapped[datetime | None] = Column(DateTime(timezone=True))
    records_generated: Mapped[int] = Column(BigInteger, default=0)
    avg_latency_ms: Mapped[int | None] = Column(Integer)
    error_message: Mapped[str | None] = Column(Text)

    # Relationships
    job: Mapped["Job"] = relationship("Job", back_populates="runs")
