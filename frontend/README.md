# Rosettix Frontend (Vite + TypeScript)

Simple TypeScript frontend to call your backend endpoints with a Vite dev server and proxy to avoid CORS.

## Endpoints wired
- Read: `POST /api/query` with `{ question, database }`
- Write: `POST /api/query/write` with `{ question, database }`
- Strategies: `GET /api/query/strategies` (fallback to `postgres`, `mongodb` if it fails)
- Health: `GET /health` and `GET /health/database`

## Run (Windows PowerShell)
```powershell
Set-Location "c:\Users\chess\rosettix-me\frontend"
npm install
npm run dev
```
Open http://localhost:5173.

## Configure
- Backend assumed at `http://localhost:8080` (see `vite.config.ts` proxy). Change if needed.
