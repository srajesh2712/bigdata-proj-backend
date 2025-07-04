from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import FileResponse
from pathlib import Path
import os

app = FastAPI()

COG_ROOT = Path(r"E:\Big Data\Summer Project\Assam-Flood-June5-2024\Flood-June5-2024\20240605")

@app.get("/{filename}")
async def serve_file(filename: str, request: Request):
    file_path = COG_ROOT / filename

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    file_size = os.path.getsize(file_path)
    range_header = request.headers.get("range")

    if range_header:
        try:
            units, range_spec = range_header.strip().split("=")
            start_str, end_str = range_spec.split("-")
            start = int(start_str)
            end = int(end_str) if end_str else file_size - 1
            chunk_size = end - start + 1
        except Exception:
            raise HTTPException(status_code=416, detail="Invalid Range header")

        with open(file_path, "rb") as f:
            f.seek(start)
            content = f.read(chunk_size)

        return Response(content, status_code=206, headers={
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(chunk_size),
            "Content-Type": "image/tiff",
        })

    return FileResponse(file_path)
