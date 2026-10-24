# =============================================================================
# Automated End-to-End Smoke Verification Script (Windows PowerShell)
# =============================================================================
$ErrorActionPreference = "Continue"

$GatewayUrl = if ($env:GATEWAY_URL) { $env:GATEWAY_URL } else { "http://localhost:8080" }
$ClickHouseUrl = if ($env:CLICKHOUSE_URL) { $env:CLICKHOUSE_URL } else { "http://localhost:8123" }
$FrontendUrl = if ($env:FRONTEND_URL) { $env:FRONTEND_URL } else { "http://localhost:5173" }

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Running Telemetry End-to-End Smoke Verification..." -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$Passed = 0
$Failed = 0

function Test-Check {
    param([string]$Name, [scriptblock]$Action)
    Write-Host -NoNewline "Checking: $Name ... "
    try {
        $result = & $Action
        if ($result -ne $false) {
            Write-Host "PASS" -ForegroundColor Green
            $script:Passed++
        } else {
            Write-Host "FAIL" -ForegroundColor Red
            $script:Failed++
        }
    } catch {
        Write-Host "FAIL ($($_))" -ForegroundColor Red
        $script:Failed++
    }
}

# 1. ClickHouse Ping
Test-Check "ClickHouse HTTP /ping" {
    $resp = Invoke-RestMethod -Uri "$ClickHouseUrl/ping" -Method Get -TimeoutSec 3
    return ($resp -match "Ok")
}

# 2. ClickHouse Tables
Test-Check "ClickHouse 'iot' database rollup tables" {
    $resp = Invoke-RestMethod -Uri "$ClickHouseUrl/?query=EXISTS+TABLE+iot.sensor_rollup_1m" -Method Get -TimeoutSec 3
    return ($resp.Trim() -eq "1")
}

# 3. Gateway AM4-Arrow endpoint
Test-Check "Gateway /api/v1/telemetry/am4-arrow returns binary stream" {
    $res = Invoke-WebRequest -Uri "$GatewayUrl/api/v1/telemetry/am4-arrow?sensorId=1&range=1h&resolution=1m" -Method Get -TimeoutSec 5
    $is200 = ($res.StatusCode -eq 200)
    $hasBytes = ($res.RawContentLength -gt 0)
    return ($is200 -and $hasBytes)
}

# 4. Frontend accessible
Test-Check "Frontend Web UI accessible" {
    $res = Invoke-WebRequest -Uri $FrontendUrl -Method Get -TimeoutSec 5
    return ($res.StatusCode -eq 200)
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Smoke Verification Summary: $Passed PASSED, $Failed FAILED" -ForegroundColor $(if ($Failed -eq 0) { "Green" } else { "Red" })
Write-Host "==========================================================" -ForegroundColor Cyan

if ($Failed -gt 0) {
    exit 1
} else {
    exit 0
}
