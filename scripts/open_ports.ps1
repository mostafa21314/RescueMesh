# Allow inbound TCP to the cloud-node ports in Windows Defender Firewall.
# Needs an administrator PowerShell (right-click > Run as administrator):
#
#   powershell -ExecutionPolicy Bypass -File scripts\open_ports.ps1            # allow 7001-7003
#   powershell -ExecutionPolicy Bypass -File scripts\open_ports.ps1 -Ports 7001-7002
#   powershell -ExecutionPolicy Bypass -File scripts\open_ports.ps1 -Remove    # delete the rule
#
# The rule covers every network profile (Domain, Private, Public), because a
# campus network is often classified as Public. Running it twice replaces the
# rule rather than adding a second one.
#
# Without admin rights, see docs/deployment.md, "Windows firewall without admin".
param(
    [string]$Ports = '7001-7003',
    [switch]$Remove
)
$ErrorActionPreference = 'Stop'
$name = 'RescueMesh cloud nodes'

$identity = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $identity.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    [Console]::Error.WriteLine('error: needs an administrator PowerShell (right-click PowerShell > Run as administrator).')
    [Console]::Error.WriteLine('       Without admin rights: start the node and accept the Windows Security Alert (docs/deployment.md).')
    exit 1
}

Get-NetFirewallRule -DisplayName $name -ErrorAction SilentlyContinue | Remove-NetFirewallRule
if ($Remove) {
    Write-Output "removed firewall rule '$name'"
    exit 0
}
New-NetFirewallRule -DisplayName $name -Direction Inbound -Protocol TCP -LocalPort $Ports `
    -Action Allow -Profile Any | Out-Null
Write-Output "firewall rule '$name': inbound TCP $Ports allowed on all profiles"
Get-NetFirewallRule -DisplayName $name | Get-NetFirewallPortFilter |
    Format-Table -AutoSize Protocol, LocalPort
