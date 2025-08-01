from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime

# --- Base Pydantic Config ---
class OrmConfig(BaseModel):
    class Config:
        orm_mode = True

# --- User Schemas ---
class UserBase(BaseModel):
    name: str
    email: EmailStr
class UserCreate(UserBase): pass
class User(UserBase, OrmConfig):
    id: UUID
    created_at: datetime

# --- Schema Field Schemas ---
class SchemaField(BaseModel):
    id: str
    name: str
    type: str
    required: bool
    description: str

# --- Schema Schemas ---
class SchemaBase(BaseModel):
    name: str
    description: Optional[str] = None
    version: Optional[str] = None
    fields: List[SchemaField]
class SchemaCreate(SchemaBase): pass
class SchemaUpdate(SchemaBase): pass
class Schema(SchemaBase, OrmConfig):
    id: UUID
    created_by: Optional[UUID] = None
    created_at: datetime
    updated_at: datetime

# --- Database Connection Schemas ---
class DatabaseConnectionBase(BaseModel):
    name: str
    type: str
    status: str = "Disconnected"
    details: str
class DatabaseConnectionCreate(DatabaseConnectionBase): pass
class DatabaseConnection(DatabaseConnectionBase, OrmConfig):
    id: UUID
    created_by: Optional[UUID] = None
    created_at: datetime

# --- Query Schemas ---
class QueryRequest(BaseModel):
    query: str
class QueryResponse(BaseModel):
    results: List[Dict[str, Any]]

# --- Job Mapping Schemas ---
class JobMappingBase(BaseModel):
    job_id: UUID
    table_name: str
    field_mappings: Dict[str, str] = Field(description="Maps schema field name to table column name")
class JobMappingCreate(JobMappingBase): pass
class JobMappingUpdate(BaseModel):
    field_mappings: Dict[str, str]
class JobMapping(JobMappingBase, OrmConfig):
    id: UUID
    created_at: datetime
    updated_at: datetime

# --- Job Schemas ---
class SimulationRules(BaseModel):
    eventType: str
    generationFrequency: int
    variabilitySettings: Optional[str] = None
class OutputSettings(BaseModel):
    outputFormat: str
    kafkaTopic: str
    metadataTags: List[str]
class JobBase(BaseModel):
    name: str
    status: str = "Draft"
    simulation_rules: Optional[SimulationRules] = None
    output_settings: Optional[OutputSettings] = None
class JobCreate(JobBase):
    schema_id: UUID
    destination_id: Optional[UUID] = None
class Job(JobBase, OrmConfig):
    id: UUID
    schema_id: UUID
    destination_id: Optional[UUID] = None
    created_by: Optional[UUID] = None
    created_at: datetime
    updated_at: datetime
    schema_definition: Optional[Schema] = None
    mappings: List[JobMapping] = []

# --- Job Run Schemas ---
class JobRunBase(BaseModel):
    status: str
    records_generated: int = 0
    avg_latency_ms: Optional[int] = None
    error_message: Optional[str] = None
class JobRunCreate(JobRunBase):
    job_id: UUID
class JobRun(JobRunBase, OrmConfig):
    id: UUID
    job_id: UUID
    started_at: datetime
    finished_at: Optional[datetime] = None

# --- Data Generation Schemas ---
class DataGenerationResponse(BaseModel):
    job_run_id: str
    status: str
    records_generated: int
    sample_data: List[Dict[str, Any]]

# --- Schemas with full relationships ---
class JobWithRuns(Job):
    runs: List[JobRun] = []
class UserWithDetails(User):
    schemas: List[Schema] = []
    connections: List[DatabaseConnection] = []
    jobs: List[Job] = []
