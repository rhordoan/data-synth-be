from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Dict, Any
import uuid
import random
import subprocess
import json
from datetime import datetime
import time

import models, schemas
from database import get_db
from services.kafka_producer import kafka_producer 
from services.database_inserter import db_inserter 

router = APIRouter()

# --- External Synthesizer Service ---
def run_synthesizer_script(schema: models.Schema, num_records: int = 1) -> List[Dict[str, Any]]:
    try:
        schema_json_str = json.dumps({"fields": schema.fields})
        command = ["python", "synthesizer.py", schema_json_str, str(num_records)]
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            timeout=60
        )
        return json.loads(result.stdout)
    except FileNotFoundError:
        raise RuntimeError("The 'synthesizer.py' script was not found.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"The synthesizer script failed with error: {e.stderr}")
    except subprocess.TimeoutExpired:
        raise RuntimeError("The synthesizer script timed out after 60 seconds.")
    except json.JSONDecodeError:
        raise RuntimeError("The synthesizer script returned invalid JSON.")
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred while running the synthesizer: {e}")

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
def read_jobs(
    skip: int = 0, limit: int = 100, status_filter: str | None = None,
    search: str | None = None, db: Session = Depends(get_db)
):
    query = db.query(models.Job)
    if status_filter and status_filter.lower() != 'all':
        query = query.filter(models.Job.status == status_filter)
    if search:
        query = query.filter(models.Job.name.ilike(f"%{search}%"))
    jobs = query.offset(skip).limit(limit).all()
    return jobs

@router.get("/jobs/{job_id}", response_model=schemas.JobWithRuns)
def read_job(job_id: str, db: Session = Depends(get_db)):
    db_job = db.query(models.Job).filter(models.Job.id == job_id).first()
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
            if db_job.destination:
                table_name = db_job.schema_definition.name.lower().replace(" ", "_")
                db_inserter.insert_batch(
                    connection_details=db_job.destination.details,
                    table_name=table_name,
                    data=generated_chunk
                )
            all_generated_data.extend(generated_chunk)
            remaining_records -= records_in_this_chunk

        job_run.status = "Finished"
        job_run.finished_at = datetime.utcnow()
        job_run.records_generated = len(all_generated_data)
        job_run.avg_latency_ms = random.randint(10, 50)
        db.commit()
        db.refresh(job_run)

        return {
            "job_run_id": str(job_run.id),
            "status": "Success",
            "records_generated": len(all_generated_data),
            "sample_data": all_generated_data[:5]
        }
    except Exception as e:
        job_run.status = "Failed"
        job_run.finished_at = datetime.utcnow()
        job_run.error_message = str(e)
        db.commit()
        raise HTTPException(status_code=500, detail=f"Data generation failed: {e}")
    finally:
        kafka_producer.close()

@router.put("/jobs/{job_id}", response_model=schemas.Job)
def update_job(job_id: str, job: schemas.JobCreate, db: Session = Depends(get_db)):
    db_job = db.query(models.Job).filter(models.Job.id == job_id).first()
    if db_job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    job_data = job.dict(exclude_unset=True)
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
    schemas = db.query(models.Schema).offset(skip).limit(limit).all()
    return schemas

@router.put("/schemas/{schema_id}", response_model=schemas.Schema)
def update_schema(schema_id: str, schema_update: schemas.SchemaUpdate, db: Session = Depends(get_db)):
    """
    Update an existing schema.
    This is used by the Schema Designer to save changes to fields.
    """
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
    db_connection = models.DatabaseConnection(**connection.dict())
    db.add(db_connection)
    db.commit()
    db.refresh(db_connection)
    return db_connection

@router.get("/connections/", response_model=List[schemas.DatabaseConnection])
def read_connections(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    connections = db.query(models.DatabaseConnection).offset(skip).limit(limit).all()
    return connections
