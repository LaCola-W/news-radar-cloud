from fastapi import FastAPI
from app.scheduler import kick_once

app = FastAPI(title="NewsRadar Cloud")

@app.get("/health")
async def health():
    return {"ok": True}

@app.post("/run")
async def run_once():
    result = await kick_once()
    return result
