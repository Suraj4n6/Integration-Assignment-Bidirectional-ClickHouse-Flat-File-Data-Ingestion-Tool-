import sys
import os
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from clickhouse_driver import Client
from file_handler import read_csv_columns
from models import ClickHouseConfig  # Make sure this model is defined in models.py
from ingestion import write_to_csv, fetch_data  # Assuming fetch_data moved to ingestion.py

# Add the current directory to system path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Function to connect to ClickHouse
def connect_clickhouse(config: ClickHouseConfig):
    try:
        client = Client(
            host=config.host,
            port=config.port,
            user=config.username,
            password=config.password,
            database=config.database,
        )
        return client
    except Exception as e:
        raise Exception(f"ClickHouse connection failed: {str(e)}")

# ✅ Route: Test connection
@app.post("/connect")
def connect_ch(config: ClickHouseConfig):
    print(f"Received config: {config}")
    try:
        client = connect_clickhouse(config)
        return {"status": "success", "message": "Connected to ClickHouse"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ✅ Route: Get tables
@app.post("/get-tables")
def get_tables(config: ClickHouseConfig):
    try:
        client = connect_clickhouse(config)
        result = client.execute("SHOW TABLES")
        tables = [row[0] for row in result]
        return {"tables": tables}
    except Exception as e:
        return {"error": str(e)}

# ✅ Route: Read CSV columns
@app.post("/get-file-columns")
async def get_file_columns(file: UploadFile = File(...), delimiter: str = Form(",")):
    try:
        return {"columns": read_csv_columns(file.file, delimiter)}
    except Exception as e:
        return {"error": str(e)}

# ✅ Route: Export ClickHouse table to CSV
@app.post("/ingest-clickhouse-to-file/")
def ingest_clickhouse_to_file(
    config: ClickHouseConfig,
    table_name: str = Form(...),
    columns: list[str] = Form(...),
):
    try:
        client = connect_clickhouse(config)
        data = fetch_data(client, table_name, columns)
        count = write_to_csv(data, columns, file_name=f"{table_name}.csv")
        return {
            "message": "Ingestion successful",
            "records_written": count,
            "file_name": f"{table_name}.csv",
        }
    except Exception as e:
        return {"error": str(e)}


