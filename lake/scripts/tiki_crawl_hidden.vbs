' Task Scheduler mở file .cmd sẽ hiện cửa sổ console; chạy qua WScript với cửa sổ ẩn (0).
Set fso = CreateObject("Scripting.FileSystemObject")
CreateObject("WScript.Shell").Run """" & fso.GetParentFolderName(WScript.ScriptFullName) & "\tiki_crawl.cmd""", 0, True
