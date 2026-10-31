# From any machine: test TCP reachability of every node in config\client.toml,
# then run `client --status`.
#
#   powershell -ExecutionPolicy Bypass -File scripts\check_cluster.ps1 [-Settle] [-TimeoutSec 3]
#
# Per node, one of:
#   reachable     something accepts connections there (the node, if the port is right)
#   refused       the machine answered but nothing listens: node not running, or wrong port
#   timeout       no answer within -TimeoutSec s (default 3): firewall or network drops it
#   unreachable   no route to host: wrong IP, a different network, or a firewall
#                 that rejects (firewalld's default)
# Windows retries a refused connection for about 2 s before reporting it, so keep
# -TimeoutSec at 3 or more, or a refused port shows up as a timeout.
# The client binary is the first of target\release\client.exe, scripts\client.exe,
# .\client.exe. Works in Windows PowerShell 5.1 and PowerShell 7.
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

function Test-Node([string]$hostName, [int]$port) {
    $tcp = New-Object System.Net.Sockets.TcpClient
    try {
        $task = $tcp.ConnectAsync($hostName, $port)
        if (-not $task.Wait($TimeoutSec * 1000)) {
            return "timeout after ${TimeoutSec}s (blocked by firewall or network)"
        }
        return 'reachable'
    } catch {
        $e = $_.Exception
        while ($e.InnerException) { $e = $e.InnerException }
        if ($e -is [System.Net.Sockets.SocketException]) {
            switch ($e.SocketErrorCode.ToString()) {
                'ConnectionRefused' { return 'refused (node not running, or wrong port)' }
                'TimedOut' { return 'timeout (blocked by firewall or network)' }
                'HostUnreachable' { return 'unreachable (wrong IP, other network, or a rejecting firewall)' }
                'NetworkUnreachable' { return 'unreachable (wrong IP, other network, or a rejecting firewall)' }
                'HostDown' { return 'unreachable (wrong IP, other network, or a rejecting firewall)' }
            }
        }
        return "error: $($e.Message)"
    } finally {
        $tcp.Close()
    }
}

Write-Output "TCP check (limit ${TimeoutSec}s per node), from ${cfg}:"
$ok = 0
$n = 1
foreach ($a in $addrs) {
    $i = $a.LastIndexOf(':')
    $hostName = $a.Substring(0, $i)
    $port = [int]$a.Substring($i + 1)
    if ($hostName -like 'MACHINE_*_IP') {
        $verdict = 'not set: run scripts\set_ips.ps1 A_IP B_IP [C_IP] first'
    } else {
        $verdict = Test-Node $hostName $port
    }
    if ($verdict -eq 'reachable') { $ok++ }
    Write-Output ('  node {0}  {1,-21} {2}' -f $n, $a, $verdict)
    $n++
}
Write-Output "$ok of $($addrs.Count) nodes reachable"

$client = $null
foreach ($b in @((Join-Path $root 'target\release\client.exe'), (Join-Path $here 'client.exe'), (Join-Path $root 'client.exe'))) {
    if (Test-Path -LiteralPath $b -PathType Leaf) { $client = $b; break }
}
if (-not $client) {
    Write-Output "no client.exe found (target\release\, scripts\ or $root); skipping client --status"
    exit 1
}
if ($ok -eq 0) {
    Write-Output 'no node reachable; skipping client --status'
    exit 1
}
Write-Output ''
Write-Output 'client --status:'
$extra = @()
if ($Settle) { $extra = @('--settle') }
& $client --config $cfg --status @extra
exit $LASTEXITCODE
