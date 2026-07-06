$Cwd = "C:\MiniProject"
Set-Location $Cwd
if (!(Test-Path "$Cwd\logs")) { New-Item -ItemType Directory -Path "$Cwd\logs" | Out-Null }
echo "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - Stopping streaming profile..." >> "$Cwd\logs\streaming_cron.log"
docker compose --profile pipeline --profile streaming --profile replay down >> "$Cwd\logs\streaming_cron.log" 2>&1
