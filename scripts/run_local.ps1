# Start the three cloud nodes on 127.0.0.1 (ports 7001-7003) from config/local/.
# Logs and pids go to logs/.
#
#   .\scripts\run_local.ps1          build (release) and start all three
#   .\scripts\run_local.ps1 stop     stop them
#
# Then, for example:
#   .\target\release\client.exe --config config\local\client.toml --count 500 --rate 100 --complexity 5
#   .\target\release\client.exe --config config\local\client.toml --status
param([string]$Action = "start")
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
New-Item -ItemType Directory -Force -Path logs | Out-Null

function Stop-Nodes {
    foreach ($i in 1..3) {
        $pidFile = "logs\node$i.pid"
        if (Test-Path $pidFile) {
            $p = Get-Content $pidFile
            Stop-Process -Id $p -ErrorAction SilentlyContinue
            Remove-Item $pidFile
            Write-Host "node $i stopped"
        }
    }
}

if ($Action -eq "stop") { Stop-Nodes; exit 0 }

Stop-Nodes
cargo build --release --quiet
if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
if (-not $env:RUST_LOG) { $env:RUST_LOG = "info" }
foreach ($i in 1..3) {
    $proc = Start-Process -FilePath "target\release\cloud-node.exe" `
        -ArgumentList "--config", "config\local\node$i.toml" `
        -RedirectStandardOutput "logs\node$i.log" -RedirectStandardError "logs\node$i.err.log" `
        -NoNewWindow -PassThru
    $proc.Id | Out-File -Encoding ascii "logs\node$i.pid"
    Write-Host "node $i started (pid $($proc.Id)), log logs\node$i.log"
}
