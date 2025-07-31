from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database import engine
import models
from api import endpoints
import uvicorn

# This command creates all the tables defined in models.py
models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Data Synthesizer API",
    description="API for managing and running synthetic data generation jobs.",
    version="1.0.0"
)

# --- CORS Configuration ---
# This allows your frontend application to make requests to this API.
origins = [
    "http://localhost",
    "http://localhost:3000",  # Default port for Next.js development
    # Add your production frontend URL here when you deploy
    # "https://your-frontend-domain.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

# Include your API routes
app.include_router(endpoints.router, prefix="/api/v1")

@app.get("/")
def read_root():
    return {"message": "Welcome to the Data Synthesizer API"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
