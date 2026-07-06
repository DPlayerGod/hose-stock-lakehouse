$Cwd = "C:\MiniProject"
Set-Location $Cwd
if (!(Test-Path "$Cwd\logs")) { New-Item -ItemType Directory -Path "$Cwd\logs" | Out-Null }
echo "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - Starting streaming profile..." >> "$Cwd\logs\streaming_cron.log"
docker compose --profile streaming up -d >> "$Cwd\logs\streaming_cron.log" 2>&1
