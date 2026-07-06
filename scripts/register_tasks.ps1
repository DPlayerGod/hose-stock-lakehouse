$StartAction = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-NoProfile -WindowStyle Hidden -File C:\MiniProject\scripts\start_streaming.ps1"
$StartTrigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 9:00AM
Register-ScheduledTask -TaskName "StockLakehouse_StartStreaming" -Action $StartAction -Trigger $StartTrigger -Description "Automatically start stock streaming profile at 9:00 AM Mon-Fri" -Force

$StopAction = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-NoProfile -WindowStyle Hidden -File C:\MiniProject\scripts\stop_streaming.ps1"
$StopTrigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 3:00PM
Register-ScheduledTask -TaskName "StockLakehouse_StopStreaming" -Action $StopAction -Trigger $StopTrigger -Description "Automatically stop stock streaming profile at 3:00 PM Mon-Fri" -Force
