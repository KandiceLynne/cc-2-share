# Ensure you're running as administrator
if (-NOT ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Error "You need to run this script as an Administrator. Right-click and select 'Run as administrator'."
    Pause
    Exit
}

# Install Chocolatey package manager if not already installed
if (-not (Get-Command choco -ErrorAction SilentlyContinue)) {
    Set-ExecutionPolicy Bypass -Scope Process -Force;
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; 
    iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))
}

# Refresh environment variables after Chocolatey installation
$env:Path = [System.Environment]::GetEnvironmentVariable('Path','Machine')

# Install common DevOps tools
choco install git -y
choco install docker-desktop -y
choco install kubernetes-cli -y
choco install terraform -y
choco install azure-cli -y
choco install awscli -y
choco install visualstudiocode -y
choco install python311 -y

Write-Host "Installation complete. Please restart your computer if necessary." -ForegroundColor Green
