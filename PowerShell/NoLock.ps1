Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process -Force

Add-Type -AssemblyName System.Windows.Forms

while ($true)

{

    [System.Windows.Forms.SendKeys]::SendWait("{F15}")

    Start-Sleep -Seconds 300

}

 
