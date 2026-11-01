# Write the machines' IP addresses into config\node1-3.toml and config\client.toml.
#
#   powershell -ExecutionPolicy Bypass -File scripts\set_ips.ps1 A_IP B_IP [C_IP]
#
# node1 runs on machine A, node3 on machine B, node2 on machine A, or on machine
# C if C_IP is given. Safe to re-run: every peer address and the client's node
# list are rewritten from the arguments (placeholders or old IPs alike), with
# each node's port taken from its own `listen` line. scripts/set_ips.sh does the
# same and writes byte-identical files (UTF-8 without BOM, LF line endings).
# Works in Windows PowerShell 5.1 and PowerShell 7.
param(
    [Parameter(Position = 0)][string]$A,
    [Parameter(Position = 1)][string]$B,
    [Parameter(Position = 2)][string]$C
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot

function Usage {
    [Console]::Error.WriteLine('usage: scripts\set_ips.ps1 A_IP B_IP [C_IP]')
    [Console]::Error.WriteLine('  node1 -> A, node2 -> A (or C if given), node3 -> B')
    exit 2
}
if (-not $A -or -not $B) { Usage }

function Test-IPv4([string]$ip) {
    if ($ip -notmatch '^([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})$') { return $false }
    foreach ($i in 1..4) { if ([int]$Matches[$i] -gt 255) { return $false } }
    return $true
}
$given = @($A, $B)
if ($C) { $given += $C }
foreach ($ip in $given) {
    if (-not (Test-IPv4 $ip)) {
        [Console]::Error.WriteLine("error: '$ip' is not an IPv4 address (e.g. 10.40.12.7)")
        exit 2
    }
    if ($ip.StartsWith('127.')) {
        [Console]::Error.WriteLine("warning: $ip is a loopback address; other machines can't reach it")
    }
}

$hosts = @('', $A, $(if ($C) { $C } else { $A }), $B)
$machines = @('', 'A', $(if ($C) { 'C' } else { 'A' }), 'B')

function Read-Lines([string]$path) {
    # Split on LF and drop any CR, so CRLF files (e.g. from a Windows checkout) work.
    $text = [System.IO.File]::ReadAllText($path)
    $lines = $text -split "`n" | ForEach-Object { $_.TrimEnd("`r") }
    if ($text.EndsWith("`n")) { $lines = $lines[0..($lines.Count - 2)] }
    return , $lines
}

# Each node's port, from its own listen line.
$ports = @('')
foreach ($k in 1..3) {
    $path = Join-Path $root "config\node$k.toml"
    $line = (Read-Lines $path) | Where-Object { $_ -match '^listen = "' } | Select-Object -First 1
    if (-not ($line -match ':([0-9]+)"')) {
        [Console]::Error.WriteLine("error: no 'listen = `"...:PORT`"' line in config\node$k.toml")
        exit 1
    }
    $ports += $Matches[1]
}

$peerRe = '^(\s*\{\s*id\s*=\s*)([0-9]+)(,\s*addr\s*=\s*")[^"]*(".*)$'
$nodesRe = '^nodes\s*=\s*\[[^\]]*\](.*)$'
$files = @('config\node1.toml', 'config\node2.toml', 'config\node3.toml', 'config\client.toml')
$out = @{}
foreach ($f in $files) {
    $new = foreach ($line in (Read-Lines (Join-Path $root $f))) {
        $peer = [regex]::Match($line, $peerRe)
        if ($peer.Success -and @('1', '2', '3') -contains $peer.Groups[2].Value) {
            $g = $peer.Groups
            $id = [int]$g[2].Value
            $g[1].Value + $g[2].Value + $g[3].Value + $hosts[$id] + ':' + $ports[$id] + $g[4].Value
        } elseif (($nodes = [regex]::Match($line, $nodesRe)).Success) {
            'nodes = ["' + $hosts[1] + ':' + $ports[1] + '", "' + $hosts[2] + ':' + $ports[2] + '", "' + $hosts[3] + ':' + $ports[3] + '"]' + $nodes.Groups[1].Value
        } else {
            $line
        }
    }
    $out[$f] = (($new -join "`n") + "`n")
}
# Build every file first, then write them all, so a failure leaves the configs untouched.
$utf8 = New-Object System.Text.UTF8Encoding $false
foreach ($f in $files) { [System.IO.File]::WriteAllText((Join-Path $root $f), $out[$f], $utf8) }

Write-Output 'node  machine  address'
foreach ($k in 1..3) {
    Write-Output ('{0,-5} {1,-8} {2}' -f $k, $machines[$k], ($hosts[$k] + ':' + $ports[$k]))
}
Write-Output ('wrote ' + (($files | ForEach-Object { $_.Replace('\', '/') }) -join ' '))
