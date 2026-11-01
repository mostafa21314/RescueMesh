# Start one cloud node in the foreground with config\node<ID>.toml.
#
#   powershell -ExecutionPolicy Bypass -File scripts\start_node.ps1 ID [-BurnThreads N]
#   powershell -ExecutionPolicy Bypass -File scripts\start_node.ps1 1
#   powershell -ExecutionPolicy Bypass -File scripts\start_node.ps1 2 -BurnThreads 4   # node 2 also burns 4 CPU threads
#
# -ExecutionPolicy Bypass applies to this one run only and needs no admin rights;
# it is needed where PowerShell refuses to run scripts ("running scripts is
# disabled on this system").
#
# The binary is the first of: target\release\cloud-node.exe (built here),
# scripts\cloud-node.exe, .\cloud-node.exe (copied, e.g. a dist\ folder). Stop
# the node with Ctrl+C. Set $env:RUST_LOG = 'debug' for more detail.
# Works in Windows PowerShell 5.1 and PowerShell 7.
param(
    [Parameter(Position = 0)][string]$Id,
    [int]$BurnThreads = 0
)
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$root = Split-Path -Parent $here

if ($Id -notmatch '^[123]$') {
    [Console]::Error.WriteLine('usage: scripts\start_node.ps1 ID [-BurnThreads N]   (ID = 1, 2 or 3)')
    exit 2
}
$Rest = @()
if ($BurnThreads -gt 0) { $Rest = @('--burn-threads', "$BurnThreads") }

$bin = $null
foreach ($b in @((Join-Path $root 'target\release\cloud-node.exe'), (Join-Path $here 'cloud-node.exe'), (Join-Path $root 'cloud-node.exe'))) {
    if (Test-Path -LiteralPath $b -PathType Leaf) { $bin = $b; break }
}
if (-not $bin) {
    [Console]::Error.WriteLine("error: no cloud-node.exe in target\release\, scripts\ or $root")
    [Console]::Error.WriteLine('       build it (cargo build --release) or copy it next to the scripts folder')
    exit 1
}

$cfg = Join-Path $root "config\node$Id.toml"
if (-not (Test-Path -LiteralPath $cfg -PathType Leaf)) {
    [Console]::Error.WriteLine("error: $cfg not found")
    exit 1
}
$lines = Get-Content -LiteralPath $cfg
if ($lines | Where-Object { $_ -notmatch '^\s*#' -and $_ -match 'MACHINE_[A-C]_IP' }) {
    [Console]::Error.WriteLine("error: $cfg still has MACHINE_*_IP placeholders; run scripts\set_ips.ps1 A_IP B_IP [C_IP] first")
    exit 1
}

Write-Output ("node ${Id}: $bin --config $cfg " + ($Rest -join ' '))
$lines | Where-Object { $_ -match '^(listen|  \{ id)' } | ForEach-Object { Write-Output "  $_" }
Write-Output 'stop with Ctrl+C'
if (-not $env:RUST_LOG) { $env:RUST_LOG = 'info' }
& $bin --config $cfg @Rest
exit $LASTEXITCODE
