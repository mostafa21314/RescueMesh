# One command per machine: build if needed, write the configs, start this
# machine's node.
#
#   powershell -ExecutionPolicy Bypass -File scripts\up.ps1 THIS_NODE NODE1_IP NODE2_IP NODE3_IP [-BurnThreads N]
#   powershell -ExecutionPolicy Bypass -File scripts\up.ps1 1 10.25.8.25 10.25.8.40 10.25.8.41
#
# The IPs are given in node order (1, 2, 3) and must be the same on every
# machine; this calls set_ips.ps1 with them in its A B C order. Opening the
# firewall needs admin: run scripts\open_ports.ps1 once, or click Allow
# (Private and Public) when Windows asks.
# Works in Windows PowerShell 5.1 and PowerShell 7.
param(
    [Parameter(Position = 0)][string]$Id,
    [Parameter(Position = 1)][string]$Ip1,
    [Parameter(Position = 2)][string]$Ip2,
    [Parameter(Position = 3)][string]$Ip3,
    [int]$BurnThreads = 0
)
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$root = Split-Path -Parent $here

if ($Id -notmatch '^[123]$' -or -not $Ip1 -or -not $Ip2 -or -not $Ip3) {
    [Console]::Error.WriteLine('usage: scripts\up.ps1 THIS_NODE NODE1_IP NODE2_IP NODE3_IP [-BurnThreads N]')
    exit 2
}
$mine = @('', $Ip1, $Ip2, $Ip3)[[int]$Id]

# Warn if this machine doesn't have the IP its node is configured for.
$have = @(Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue | ForEach-Object { $_.IPAddress })
if ($have -notcontains $mine) {
    [Console]::Error.WriteLine("warning: node $Id is set to $mine, but this machine has: $($have -join ' ')")
}

$bins = @((Join-Path $root 'target\release\cloud-node.exe'), (Join-Path $here 'cloud-node.exe'), (Join-Path $root 'cloud-node.exe'))
if (-not ($bins | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf })) {
    # The GNU toolchain needs MinGW's dlltool and as; winget's WinLibs isn't always on PATH.
    $winlibs = Get-ChildItem "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" -Directory -Filter '*WinLibs*' -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($winlibs) { $env:Path = (Join-Path $winlibs.FullName 'mingw64\bin') + ";$env:Path" }
    $env:Path = "$env:USERPROFILE\.cargo\bin;$env:Path"
    Write-Output 'building (cargo build --release)...'
    Push-Location $root
    try { cargo build --release; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } } finally { Pop-Location }
}

# set_ips.ps1 only sets an exit code on failure, so clear any earlier one first.
$global:LASTEXITCODE = 0
& (Join-Path $here 'set_ips.ps1') $Ip1 $Ip3 $Ip2
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& (Join-Path $here 'start_node.ps1') $Id -BurnThreads $BurnThreads
exit $LASTEXITCODE