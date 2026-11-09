param(
    [switch]$Repair
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root
$Marker = Join-Path $Root ".setup_complete.json"

Write-Host "============================================================"
Write-Host "              ASADS AUTOMATION SETUP v0.7.8 HARDENED"
Write-Host "============================================================"

if ((Test-Path $Marker) -and -not $Repair) {
    Write-Host "Setup was already completed on this PC."
    Write-Host "Normal BOT launches do not reinstall Python or packages."
    Write-Host "Use REPAIR_SETUP.cmd only if the environment was damaged."
    exit 0
}

function Test-PythonCandidate([string]$Exe, [string[]]$PrefixArgs) {
    try {
        $args = @($PrefixArgs) + @('-c', 'import sys; print(sys.executable); raise SystemExit(0 if sys.version_info >= (3,11) else 2)')
        $out = & $Exe @args 2>$null
        if ($LASTEXITCODE -eq 0) {
            return @{ Exe=$Exe; Prefix=$PrefixArgs; Display=($out | Select-Object -First 1) }
        }
    } catch {}
    return $null
}

Write-Host "[1/7] Checking Python..."
$Python = $null
$Py = Get-Command py -ErrorAction SilentlyContinue
if ($Py) {
    $Python = Test-PythonCandidate $Py.Source @('-3.13')
    if (-not $Python) { $Python = Test-PythonCandidate $Py.Source @('-3') }
}
if (-not $Python) {
    $PythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($PythonCmd) { $Python = Test-PythonCandidate $PythonCmd.Source @() }
}
if (-not $Python) {
    $Winget = Get-Command winget -ErrorAction SilentlyContinue
    if (-not $Winget) {
        throw "Python 3.11+ was not found and Windows Package Manager is unavailable. Install Python 3.13, then rerun SETUP_ONCE.cmd."
    }
    Write-Host "Python 3.11+ not found. Installing official Python 3.13 with winget..."
    & $Winget.Source install --id Python.Python.3.13 -e --accept-package-agreements --accept-source-agreements
    if ($LASTEXITCODE -ne 0) { throw "Python installation failed." }
    $Py = Get-Command py -ErrorAction SilentlyContinue
    if ($Py) { $Python = Test-PythonCandidate $Py.Source @('-3.13') }
    if (-not $Python) { throw "Python installed but could not be located. Reopen Windows Terminal and rerun SETUP_ONCE.cmd." }
}
Write-Host "Using Python: $($Python.Display)"

Write-Host "[2/7] Creating/repairing isolated BOT environment..."
if (-not (Test-Path ".venv\Scripts\python.exe")) {
    & $Python.Exe @($Python.Prefix) -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw "Could not create .venv." }
}
$BotPy = Join-Path $Root ".venv\Scripts\python.exe"

Write-Host "[3/7] Installing BOT dependencies into .venv..."
& $BotPy -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed." }
& $BotPy -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }
& $BotPy -c "from zoneinfo import ZoneInfo; ZoneInfo('America/New_York'); print('Timezone database OK')"
if ($LASTEXITCODE -ne 0) { throw "Timezone database verification failed." }

Write-Host "[4/7] Verifying Cloudflare Tunnel client..."
$Cloudflared = Join-Path $Root "tools\cloudflared.exe"
# SETUP/REPAIR always validates the existing binary (or installs it if absent).
# Normal BOT startup never runs this installer path.
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Root "tools\setup_cloudflared.ps1")
if ($LASTEXITCODE -ne 0) { throw "cloudflared setup/signature verification failed." }
& $Cloudflared --version
if ($LASTEXITCODE -ne 0) { throw "cloudflared verification failed." }

Write-Host "[5/7] Compiling BOT Python files..."
& $BotPy -m compileall -q app tools
if ($LASTEXITCODE -ne 0) { throw "BOT Python compile check failed." }

Write-Host "[6/7] Connecting/validating Robinhood Agentic Trading (READ-ONLY)..."
Write-Host "A browser authorization window may open. Sign in to THIS PC owner's Robinhood account."
& $BotPy (Join-Path $Root "tools\test_robinhood_mcp.py")
if ($LASTEXITCODE -ne 0) { throw "Robinhood MCP read-only validation failed. No setup-complete marker was written." }

Write-Host "[7/7] Creating desktop shortcut..."
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Root "tools\install_bot_shortcut.ps1")
if ($LASTEXITCODE -ne 0) { throw "Shortcut creation failed." }

$markerPayload = @{
    version = "0.7.8-hardened"
    completed_at = (Get-Date).ToString("o")
    python = $Python.Display
    repair = [bool]$Repair
} | ConvertTo-Json
Set-Content -Path $Marker -Value $markerPayload -Encoding UTF8

Write-Host ""
Write-Host "SETUP COMPLETE."
Write-Host "From now on, use BOT.cmd or the desktop BOT shortcut."
Write-Host "Normal startup does NOT run pip or reinstall Python."
Write-Host "Use REPAIR_SETUP.cmd only if setup files/dependencies become damaged."
