# Query the real UDP request/reply protocol; a successful UDP connect alone does not prove reachability.
param(
    [switch]$Settle,
    [int]$TimeoutSec = 3
)
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$root = Split-Path -Parent $here
$cfg = Join-Path $root 'config\client.toml'

if (-not (Test-Path -LiteralPath $cfg -PathType Leaf)) {
    [Console]::Error.WriteLine("error: $cfg not found")
    exit 1
}
$line = Get-Content -LiteralPath $cfg | Where-Object { $_ -match '^nodes\s*=' } | Select-Object -First 1
$addrs = @()
if ($line) { $addrs = @([regex]::Matches($line, '"([^"]+)"') | ForEach-Object { $_.Groups[1].Value }) }
if ($addrs.Count -eq 0) {
    [Console]::Error.WriteLine("error: no 'nodes = [`"host:port`", ...]' line in $cfg")
    exit 1
}

$client = $null
foreach ($b in @((Join-Path $root 'target\release\client.exe'), (Join-Path $here 'client.exe'), (Join-Path $root 'client.exe'))) {
    if (Test-Path -LiteralPath $b -PathType Leaf) { $client = $b; break }
}
if (-not $client) {
    Write-Output "no client.exe found (target\release\, scripts\ or $root); skipping client --status"
    exit 1
}
Write-Output ''
Write-Output 'client --status:'
$extra = @()
if ($Settle) { $extra = @('--settle') }
& $client --config $cfg --status @extra
exit $LASTEXITCODE
