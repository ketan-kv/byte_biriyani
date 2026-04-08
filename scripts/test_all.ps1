param(
    [switch]$SkipApiSmoke
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptRoot "..")
Set-Location $projectRoot

$candidatePython = @(
    (Join-Path $projectRoot ".venv\Scripts\python.exe"),
    (Join-Path (Resolve-Path (Join-Path $projectRoot "..")).Path ".venv\Scripts\python.exe")
)

$python = $candidatePython | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $python) {
    throw "Python environment not found. Checked: $($candidatePython -join ', ')"
}

function Run-Step {
    param(
        [string]$Name,
        [scriptblock]$Command
    )

    Write-Host "`n==> $Name" -ForegroundColor Cyan
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Step failed: $Name"
    }
}

Run-Step "Run full pytest suite" {
    & $python -m pytest -q
}

if (-not $SkipApiSmoke) {
    $apiSmoke = @'
from api.main import create_app
from agents.orchestrator_agent import OrchestratorAgent
from utils.config_loader import load_config
from storage.db import init_db
from fastapi.testclient import TestClient

cfg = load_config()
init_db(cfg["paths"]["sqlite_path"])
app = create_app(OrchestratorAgent(cfg), cfg)
client = TestClient(app)

checks = {
    "health": client.get("/health").status_code,
    "ingest": client.post(
        "/ingest",
        json={"path": "tests/fixtures/sample_production.csv", "file_type": "production_log"},
    ).status_code,
    "run_pipeline": client.post("/run-pipeline").status_code,
    "insights": client.get("/insights").status_code,
    "analytics": client.get("/analytics/descriptive").status_code,
}

missing = client.post(
    "/ingest",
    json={"path": "tests/fixtures/does_not_exist.csv", "file_type": "sensor_csv"},
)

if not all(code == 200 for code in checks.values()):
    raise SystemExit(f"API smoke failed: {checks}")
if missing.status_code != 404:
    raise SystemExit(f"Expected 404 for missing file ingest, got {missing.status_code}")

print("API smoke PASS", checks)
'@

    Run-Step "Run API smoke checks (FastAPI TestClient)" {
        $tempPy = Join-Path ([System.IO.Path]::GetTempPath()) ("amdais_api_smoke_{0}.py" -f ([guid]::NewGuid().ToString("N")))
        Set-Content -Path $tempPy -Value $apiSmoke -Encoding utf8
        $oldPythonPath = $env:PYTHONPATH
        $env:PYTHONPATH = if ([string]::IsNullOrWhiteSpace($oldPythonPath)) {
            $projectRoot.Path
        }
        else {
            "$($projectRoot.Path);$oldPythonPath"
        }
        try {
            & $python $tempPy
        }
        finally {
            $env:PYTHONPATH = $oldPythonPath
            Remove-Item $tempPy -ErrorAction SilentlyContinue
        }
    }
}

Write-Host "`nAll checks passed." -ForegroundColor Green
