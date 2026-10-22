param([string]$At = "02:00", [switch]$RunNow)

$vbs = Join-Path $PSScriptRoot "tiki_crawl_hidden.vbs"
$action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument "`"$vbs`"" -WorkingDirectory (Split-Path $PSScriptRoot)
$trigger = New-ScheduledTaskTrigger -Daily -At $At
# StartWhenAvailable: máy tắt lúc 02:00 thì chạy bù khi bật lại. IgnoreNew: không chồng 2 lần chạy.
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 20)
Register-ScheduledTask -TaskName "BTL Tiki crawl" -Action $action -Trigger $trigger -Settings $settings `
    -Description "Crawl Tiki thời trang hằng ngày (python run.py crawl), log ở data\_logs\tiki_crawl_*.log" -Force | Out-Null
Write-Host "Đã đăng ký task 'BTL Tiki crawl' chạy hằng ngày lúc $At"
if ($RunNow) { Start-ScheduledTask -TaskName "BTL Tiki crawl"; Write-Host "Đã chạy ngay" }
