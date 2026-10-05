# Seed dữ liệu Olist + Clickstream vào PostgreSQL (Docker Compose)
# Dataset được mount read-only vào postgres:/data, Script/ vào postgres:/sql
# Chạy từ thư mục lakehouse_docker:  .\scripts\seed-data.ps1

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "Checking PostgreSQL container..." -ForegroundColor Yellow
docker compose exec -T postgres pg_isready -U postgres -d orders
if ($LASTEXITCODE -ne 0) {
    Write-Error "PostgreSQL is not ready! Run 'docker compose up -d' first."
    exit 1
}

# Các file SQL dùng đường dẫn /tmp/olist và /tmp/clickstream → symlink sang /data
docker compose exec -T postgres sh -c "ln -sfn /data/olist /tmp/olist && ln -sfn /data/clickstream /tmp/clickstream"

$Scripts = @(
    "create_tables.sql",
    "import_raw.sql",
    "create_clickstream_tables.sql",
    "import_clickstream.sql"
)
foreach ($s in $Scripts) {
    Write-Host "Running $s ..." -ForegroundColor Yellow
    docker compose exec -T postgres psql -U postgres -d orders -f "/sql/$s"
    if ($LASTEXITCODE -ne 0) {
        Write-Error "$s failed."
        exit 1
    }
}

Write-Host "PostgreSQL Data Seeding Completed Successfully!" -ForegroundColor Green
