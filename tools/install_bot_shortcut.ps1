$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Desktop = [Environment]::GetFolderPath("Desktop")
$ShortcutPath = Join-Path $Desktop "Asads AUTOMATION.lnk"
$Script = Join-Path $Root "tools\bot_app.py"
$VenvPythonW = Join-Path $Root ".venv\Scripts\pythonw.exe"
$VenvPython = Join-Path $Root ".venv\Scripts\python.exe"

if (-not (Test-Path $Script)) {
    throw "BOT dashboard script was not found: $Script"
}

if (Test-Path $VenvPythonW) {
    $Target = $VenvPythonW
} elseif (Test-Path $VenvPython) {
    $Target = $VenvPython
} else {
    $Py = Get-Command pythonw -ErrorAction SilentlyContinue
    if ($Py) {
        $Target = $Py.Source
    } else {
        $Py = Get-Command python -ErrorAction Stop
        $Target = $Py.Source
    }
}

$Shell = New-Object -ComObject WScript.Shell
$Shortcut = $Shell.CreateShortcut($ShortcutPath)
$Shortcut.TargetPath = $Target
$Shortcut.Arguments = '"' + $Script + '"'
$Shortcut.WorkingDirectory = $Root
$Shortcut.Description = "Open Asads AUTOMATION"
$Shortcut.IconLocation = "$env:SystemRoot\System32\shell32.dll,44"
$Shortcut.Save()

Write-Host "Desktop shortcut created: $ShortcutPath"
Write-Host "Double-click Asads AUTOMATION to start the dashboard, Robinhood MCP worker, and Cloudflare tunnel."
