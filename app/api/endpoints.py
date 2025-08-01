from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict, Any, AsyncGenerator
import uuid
import random
import subprocess
import json
from datetime import datetime
import asyncio

import models, schemas
from database import get_db
from services.kafka_producer import kafka_producer 
from services.database_inserter import db_inserter 

router = APIRouter()

# --- Helper Function for DB Connection ---
def check_db_connection(connection_string: str) -> bool:
    """Tries to establish a connection to the database."""
    try:
        if connection_string.startswith("sqlite"):
            engine = create_engine(connection_string, connect_args={"check_same_thread": False})
        else:
            engine = create_engine(connection_string)
        with engine.connect():
            return True
    except SQLAlchemyError as e:
        print(f"Connection check failed: {e}")
        return False

# --- External Synthesizer Service ---
def run_synthesizer_script(schema: models.Schema, num_records: int = 1) -> List[Dict[str, Any]]:
    """Executes the external synthesizer script as a separate process."""
    try:
        schema_json_str = json.dumps({"fields": schema.fields})
        command = ["python", "synthesizer.py", schema_json_str, str(num_records)]
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=120)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Synthesizer script failed with error: {e.stderr}")
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred while running the synthesizer: {e}")

# --- Real-time Stream Generator ---
async def stream_generator(job_id: str, db: Session) -> AsyncGenerator[str, None]:
    """
    Asynchronous generator function for the SSE stream.
    This will generate one record at a time and yield it to the client.
    """
    print(f"Starting stream for job ID: {job_id}")
    db_job = db.query(models.Job).options(joinedload(models.Job.schema_definition)).filter(models.Job.id == job_id).first()
    if not db_job:
        print(f"Attempted to stream for non-existent job ID: {job_id}")
        return

    try:
        while True:
            generated_record = run_synthesizer_script(db_job.schema_definition, num_records=1)
            if generated_record:
                yield f"data: {json.dumps(generated_record[0])}\n\n"
            
            frequency = db_job.simulation_rules.get("generationFrequency", 1) if db_job.simulation_rules else 1
            delay = 1.0 / frequency if frequency > 0 else 1
            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        print(f"Client disconnected from job stream: {job_id}")
    except Exception as e:
        print(f"Error during job stream {job_id}: {e}")
    finally:
        print(f"Closing stream for job {job_id}")

# --- Job Endpoints ---
@router.post("/jobs/", response_model=schemas.Job, status_code=status.HTTP_201_CREATED)
def create_job(job: schemas.JobCreate, db: Session = Depends(get_db)):
    db_schema = db.query(models.Schema).filter(models.Schema.id == str(job.schema_id)).first()
    if not db_schema:
        raise HTTPException(status_code=404, detail="Schema not found")
    if job.destination_id:
        db_destination = db.query(models.DatabaseConnection).filter(models.DatabaseConnection.id == str(job.destination_id)).first()
        if not db_destination:
            raise HTTPException(status_code=404, detail="Database connection not found")
    db_job = models.Job(
        name=job.name, status=job.status, schema_id=str(job.schema_id),
        destination_id=str(job.destination_id) if job.destination_id else None,
        simulation_rules=job.simulation_rules.dict() if job.simulation_rules else None,
        output_settings=job.output_settings.dict() if job.output_settings else None
    )
    db.add(db_job)
    db.commit()
    db.refresh(db_job)
    return db_job

@router.get("/jobs/", response_model=List[schemas.Job])
def read_jobs(skip: int = 0, limit: int = 100, status_filter: str | None = None, search: str | None = None, db: Session = Depends(get_db)):
    query = db.query(models.Job).options(
        joinedload(models.Job.schema_definition),
        joinedload(models.Job.destination)
    )
    if status_filter and status_filter.lower() != 'all':
        query = query.filter(models.Job.status == status_filter)
    if search:
        query = query.filter(models.Job.name.ilike(f"%{search}%"))
    jobs = query.offset(skip).limit(limit).all()
    return jobs

@router.get("/jobs/{job_id}", response_model=schemas.JobWithRuns)
def read_job(job_id: str, db: Session = Depends(get_db)):
    db_job = db.query(models.Job).options(
        joinedload(models.Job.schema_definition),
        joinedload(models.Job.destination),
        joinedload(models.Job.runs)
    ).filter(models.Job.id == job_id).first()
    if db_job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return db_job
    
@router.post("/jobs/{job_id}/generate", response_model=schemas.DataGenerationResponse)
def generate_data_for_job(job_id: str, db: Session = Depends(get_db)):
    db_job = db.query(models.Job).filter(models.Job.id == job_id).first()
    if db_job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    job_run = models.JobRun(job_id=job_id, status="Started")
    db.add(job_run)
    db.commit()
    db.refresh(job_run)

    try:
        total_records_to_generate = db_job.simulation_rules.get("generationFrequency", 1) if db_job.simulation_rules else 1
        CHUNK_SIZE = 20
        all_generated_data = []
        kafka_topic = db_job.output_settings.get("kafkaTopic", "default-topic") if db_job.output_settings else "default-topic"
        
        remaining_records = total_records_to_generate
        while remaining_records > 0:
            records_in_this_chunk = min(remaining_records, CHUNK_SIZE)
            generated_chunk = run_synthesizer_script(db_job.schema_definition, num_records=records_in_this_chunk)
            
            kafka_producer.send(topic=kafka_topic, data=generated_chunk)

            if db_job.destination and db_job.mappings:
                for mapping in db_job.mappings:
                    table_name = mapping.table_name
                    field_mappings = mapping.field_mappings
                    
                    transformed_chunk = []
                    for record in generated_chunk:
                        transformed_record = {db_column: record.get(schema_field) for schema_field, db_column in field_mappings.items() if schema_field in record}
                        if transformed_record:
                            transformed_chunk.append(transformed_record)
                    
                    if transformed_chunk:
                        db_inserter.insert_batch(connection_details=db_job.destination.details, table_name=table_name, data=transformed_chunk)
            
            all_generated_data.extend(generated_chunk)
            remaining_records -= records_in_this_chunk

        job_run.status = "Finished"
        job_run.finished_at = datetime.utcnow()
        job_run.records_generated = len(all_generated_data)
        job_run.avg_latency_ms = random.randint(10, 50)
        db.commit()
        db.refresh(job_run)
        return {"job_run_id": str(job_run.id), "status": "Success", "records_generated": len(all_generated_data), "sample_data": all_generated_data[:5]}
    except Exception as e:
        error_message = str(e)
        job_run.status = "Failed"
        job_run.finished_at = datetime.utcnow()
        job_run.error_message = error_message
        db.commit()
        raise HTTPException(status_code=500, detail=error_message)
    finally:
        kafka_producer.close()

@router.get("/jobs/{job_id}/stream")
async def stream_job_data(job_id: str, db: Session = Depends(get_db)):
    """
    Endpoint to stream generated data for a job in real-time using SSE.
    """
    return StreamingResponse(stream_generator(job_id, db), media_type="text/event-stream")

@router.put("/jobs/{job_id}", response_model=schemas.Job)
def update_job(job_id: str, job: schemas.JobCreate, db: Session = Depends(get_db)):
    db_job = db.query(models.Job).filter(models.Job.id == job_id).first()
    if db_job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    job_data = job.dict(exclude_unset=True)
    if 'schema_id' in job_data: job_data['schema_id'] = str(job_data['schema_id'])
    if 'destination_id' in job_data and job_data['destination_id'] is not None: job_data['destination_id'] = str(job_data['destination_id'])
    for key, value in job_data.items():
        setattr(db_job, key, value)
    db.commit()
    db.refresh(db_job)
    return db_job

@router.delete("/jobs/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_job(job_id: str, db: Session = Depends(get_db)):
    db_job = db.query(models.Job).filter(models.Job.id == job_id).first()
    if db_job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    db.delete(db_job)
    db.commit()
    return

# --- Schema Endpoints ---
@router.post("/schemas/", response_model=schemas.Schema, status_code=status.HTTP_201_CREATED)
def create_schema(schema: schemas.SchemaCreate, db: Session = Depends(get_db)):
    db_schema = models.Schema(**schema.dict())
    db.add(db_schema)
    db.commit()
    db.refresh(db_schema)
    return db_schema

@router.get("/schemas/", response_model=List[schemas.Schema])
def read_schemas(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return db.query(models.Schema).offset(skip).limit(limit).all()

@router.put("/schemas/{schema_id}", response_model=schemas.Schema)
def update_schema(schema_id: str, schema_update: schemas.SchemaUpdate, db: Session = Depends(get_db)):
    db_schema = db.query(models.Schema).filter(models.Schema.id == schema_id).first()
    if db_schema is None:
        raise HTTPException(status_code=404, detail="Schema not found")
    update_data = schema_update.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_schema, key, value)
    db.commit()
    db.refresh(db_schema)
    return db_schema

# --- Database Connection Endpoints ---
@router.post("/connections/", response_model=schemas.DatabaseConnection, status_code=status.HTTP_201_CREATED)
def create_connection(connection: schemas.DatabaseConnectionCreate, db: Session = Depends(get_db)):
    if not check_db_connection(connection.details):
        raise HTTPException(status_code=400, detail="Invalid connection string or database is unreachable.")
    connection_data = connection.dict()
    connection_data['status'] = 'Connected'
    db_connection = models.DatabaseConnection(**connection_data)
    db.add(db_connection)
    db.commit()
    db.refresh(db_connection)
    return db_connection

@router.get("/connections/", response_model=List[schemas.DatabaseConnection])
def read_connections(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return db.query(models.DatabaseConnection).offset(skip).limit(limit).all()

@router.get("/connections/{connection_id}/tables", response_model=List[str])
def get_tables_for_connection(connection_id: str, db: Session = Depends(get_db)):
    db_connection = db.query(models.DatabaseConnection).filter(models.DatabaseConnection.id == connection_id).first()
    if not db_connection:
        raise HTTPException(status_code=404, detail="Database connection not found")
    try:
        engine_args = {"connect_args": {"check_same_thread": False}} if db_connection.details.startswith("sqlite") else {}
        target_engine = create_engine(db_connection.details, **engine_args)
        inspector = inspect(target_engine)
        tables = inspector.get_table_names()
        target_engine.dispose()
        return tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not retrieve tables: {e}")

@router.get("/connections/{connection_id}/tables/{table_name}/columns", response_model=List[str])
def get_columns_for_table(connection_id: str, table_name: str, db: Session = Depends(get_db)):
    db_connection = db.query(models.DatabaseConnection).filter(models.DatabaseConnection.id == connection_id).first()
    if not db_connection:
        raise HTTPException(status_code=404, detail="Database connection not found")
    try:
        engine_args = {"connect_args": {"check_same_thread": False}} if db_connection.details.startswith("sqlite") else {}
        target_engine = create_engine(db_connection.details, **engine_args)
        inspector = inspect(target_engine)
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        target_engine.dispose()
        return columns
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not retrieve columns for table {table_name}: {e}")

@router.post("/connections/{connection_id}/query", response_model=schemas.QueryResponse)
def execute_query(connection_id: str, query_request: schemas.QueryRequest, db: Session = Depends(get_db)):
    db_connection = db.query(models.DatabaseConnection).filter(models.DatabaseConnection.id == connection_id).first()
    if not db_connection:
        raise HTTPException(status_code=404, detail="Database connection not found")
    try:
        engine_args = {"connect_args": {"check_same_thread": False}} if db_connection.details.startswith("sqlite") else {}
        target_engine = create_engine(db_connection.details, **engine_args)
        with target_engine.connect() as connection:
            with connection.begin():
                result = connection.execute(text(query_request.query))
                results_as_dict = [row._asdict() for row in result]
        target_engine.dispose()
        return {"results": results_as_dict}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.delete("/connections/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_connection(connection_id: str, db: Session = Depends(get_db)):
    db_connection = db.query(models.DatabaseConnection).filter(models.DatabaseConnection.id == connection_id).first()
    if db_connection is None:
        raise HTTPException(status_code=404, detail="Connection not found")
    db.delete(db_connection)
    db.commit()
    return

# --- Job Mapping Endpoints ---
@router.post("/job_mappings/", response_model=schemas.JobMapping, status_code=status.HTTP_201_CREATED)
def create_job_mapping(job_mapping: schemas.JobMappingCreate, db: Session = Depends(get_db)):
    db_job = db.query(models.Job).filter(models.Job.id == str(job_mapping.job_id)).first()
    if not db_job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    existing_mapping = db.query(models.JobMapping).filter_by(job_id=str(job_mapping.job_id), table_name=job_mapping.table_name).first()
    if existing_mapping:
        raise HTTPException(status_code=400, detail=f"Mapping for this job and table already exists.")

    mapping_data = job_mapping.dict()
    mapping_data['job_id'] = str(mapping_data['job_id'])
    
    db_mapping = models.JobMapping(**mapping_data)
    db.add(db_mapping)
    db.commit()
    db.refresh(db_mapping)
    return db_mapping

@router.get("/jobs/{job_id}/mappings", response_model=List[schemas.JobMapping])
def get_mappings_for_job(job_id: str, db: Session = Depends(get_db)):
    db_job = db.query(models.Job).filter(models.Job.id == job_id).first()
    if not db_job:
        raise HTTPException(status_code=404, detail="Job not found")
    return db_job.mappings

@router.put("/job_mappings/{mapping_id}", response_model=schemas.JobMapping)
def update_job_mapping(mapping_id: str, mapping_update: schemas.JobMappingUpdate, db: Session = Depends(get_db)):
    db_mapping = db.query(models.JobMapping).filter(models.JobMapping.id == mapping_id).first()
    if not db_mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")
    
    db_mapping.field_mappings = mapping_update.field_mappings
    db.commit()
    db.refresh(db_mapping)
    return db_mapping
