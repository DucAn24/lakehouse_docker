# Đăng ký lại Debezium connectors (connectors/*.json) — chạy từ bất kỳ đâu
Set-Location (Join-Path $PSScriptRoot "..")
docker compose run --rm connect-init
Write-Host "`nCurrent connectors:" -ForegroundColor Cyan
curl.exe -s http://localhost:8083/connectors
