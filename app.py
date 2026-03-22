import os
import webbrowser
import threading
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn
import time

from src.db_connector import DBConnector

app = FastAPI()

# Make sure static directory exists
os.makedirs("static", exist_ok=True)

app.mount("/static", StaticFiles(directory="static"), name="static")

class ConnectRequest(BaseModel):
    sourceHost: str
    sourcePort: int
    sourceService: str
    sourceUser: str
    sourcePass: str
    targetHost: str
    targetPort: int
    targetService: str
    targetUser: str
    targetPass: str

class TestConnectRequest(BaseModel):
    dbType: str
    host: str
    port: int
    service: str
    user: str
    password: str

@app.post("/api/test_connect")
async def test_connect(req: TestConnectRequest):
    config = {
        'host': req.host,
        'port': req.port,
        'service_name': req.service,
        'user': req.user,
        'password': req.password
    }
    db = DBConnector(config)
    ok = db.test_connection(db_type="oracle")
    if ok:
        return {"status": "success", "message": f"Successfully connected to {req.dbType.capitalize()} DB."}
    else:
        return {"status": "error", "message": f"Failed to connect to {req.dbType.capitalize()} DB. Check your credentials and try again."}

@app.post("/api/connect")
async def connect_dbs(req: ConnectRequest):
    # Test Source DB
    source_config = {
        'host': req.sourceHost,
        'port': req.sourcePort,
        'service_name': req.sourceService,
        'user': req.sourceUser,
        'password': req.sourcePass
    }
    source_db = DBConnector(source_config)
    
    # Test Target DB
    target_config = {
        'host': req.targetHost,
        'port': req.targetPort,
        'service_name': req.targetService,
        'user': req.targetUser,
        'password': req.targetPass
    }
    target_db = DBConnector(target_config)
    
    source_ok = source_db.test_connection(db_type="oracle")
    if not source_ok:
        return {"status": "error", "message": "Failed to connect to Source Database."}
        
    target_ok = target_db.test_connection(db_type="oracle")
    if not target_ok:
        return {"status": "error", "message": "Failed to connect to Target Database."}
        
    return {"status": "success", "message": "Both Source and Target DBs connected successfully."}

@app.get("/", response_class=HTMLResponse)
async def get_index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()

# Global Migration State
migration_state = {
    "total_tables": 1240,
    "transfer_rate": "0 rows/s",
    "elapsed_time": "00:00:00",
    "elapsed_seconds": 0,
    "progress": 0,
    "syncing_tables": [
        {"name": "TELEPORT_SRC", "status": "Syncing...", "rows": "0", "speed": "1.2M/s", "progress": 0},
        {"name": "user_auth", "status": "Pending", "rows": "0", "speed": "0.5M/s", "progress": 0},
        {"name": "db_prod_main", "status": "Pending", "rows": "0", "speed": "2.1M/s", "progress": 0},
    ]
}

def run_migration_simulation():
    global migration_state
    while True:
        time.sleep(1.5)
        if migration_state["progress"] >= 100:
            migration_state["transfer_rate"] = "0 rows/s"
            for t in migration_state["syncing_tables"]:
                t["status"] = "Completed"
                t["progress"] = 100
            break
            
        migration_state["elapsed_seconds"] += 1.5
        mins = int(migration_state["elapsed_seconds"] // 60)
        secs = int(migration_state["elapsed_seconds"] % 60)
        migration_state["elapsed_time"] = f"{mins:02d}:{secs:02d}"
        
        migration_state["progress"] += 5
        migration_state["progress"] = min(100, migration_state["progress"])
        migration_state["transfer_rate"] = f"{round(1.0 + (migration_state['progress']/30), 1)}M rows/s"
        
        # Table 1 Simulator
        if migration_state["progress"] <= 50:
            p = min(100, migration_state["progress"] * 2)
            migration_state["syncing_tables"][0]["progress"] = p
            migration_state["syncing_tables"][0]["rows"] = f"{int(12040102 * p / 100):,}"
            if p >= 100:
                migration_state["syncing_tables"][0]["status"] = "Completed"
                
        # Table 2 Simulator
        if 40 <= migration_state["progress"] <= 80:
            p = min(100, (migration_state["progress"] - 40) * 2.5)
            migration_state["syncing_tables"][1]["status"] = "Syncing..."
            migration_state["syncing_tables"][1]["progress"] = p
            migration_state["syncing_tables"][1]["rows"] = f"{int(1450000 * p / 100):,}"
            if p >= 100:
                migration_state["syncing_tables"][1]["status"] = "Completed"
                
        # Table 3 Simulator
        if migration_state["progress"] >= 70:
            p = min(100, (migration_state["progress"] - 70) * 3.33)
            migration_state["syncing_tables"][2]["status"] = "Syncing..."
            migration_state["syncing_tables"][2]["progress"] = p
            migration_state["syncing_tables"][2]["rows"] = f"{int(8192000 * p / 100):,}"
            if p >= 100:
                migration_state["syncing_tables"][2]["status"] = "Completed"

simulation_thread = None

@app.post("/api/start")
async def start_migration(request: Request):
    global simulation_thread, migration_state
    
    # State reset
    migration_state["progress"] = 0
    migration_state["elapsed_seconds"] = 0
    migration_state["elapsed_time"] = "00:00:00"
    migration_state["transfer_rate"] = "0 rows/s"
    for t in migration_state["syncing_tables"]:
        t["status"] = "Pending"
        t["progress"] = 0
        t["rows"] = "0"

    if simulation_thread is None or not simulation_thread.is_alive():
        simulation_thread = threading.Thread(target=run_migration_simulation, daemon=True)
        simulation_thread.start()
        
    return {"status": "success", "message": "Migration started"}

@app.get("/api/metrics")
async def get_metrics():
    # Return real-time simulation state
    return migration_state

def open_browser():
    time.sleep(1.5)
    webbrowser.open("http://127.0.0.1:8000")

if __name__ == "__main__":
    threading.Thread(target=open_browser, daemon=True).start()
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
