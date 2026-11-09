$ErrorActionPreference = "Stop"

$ToolsDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Target = Join-Path $ToolsDir "cloudflared.exe"
$Url = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"

function Assert-CloudflaredSignature([string]$Path) {
    $sig = Get-AuthenticodeSignature -FilePath $Path
    if ($sig.Status -ne "Valid") {
        throw "cloudflared signature check failed: $($sig.Status)"
    }
}

if (Test-Path $Target) {
    Assert-CloudflaredSignature $Target
    Write-Host "cloudflared verified at: $Target"
    & $Target --version
    exit 0
}

Write-Host "Downloading official Cloudflare Tunnel client..."
Invoke-WebRequest -Uri $Url -OutFile $Target -UseBasicParsing
Assert-CloudflaredSignature $Target
Write-Host "Downloaded and signature-verified: $Target"
& $Target --version
