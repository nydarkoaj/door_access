# Authenticate to Microsoft Graph with required scopes
Connect-MgGraph -Scopes "Sites.Read.All", "Files.Read.All"

# Get the target SharePoint site by site path
$site = Get-MgSite -SiteId "amalitech.sharepoint.com:/sites/global/chapters"

# Get the drive (document library) associated with the site
$drive = Get-MgSiteDrive -SiteId $site.Id

# Get the root folder of the drive
$root = Get-MgDriveRoot -DriveId $drive.Id


# Get the 'Data Science' folder by its name
$dataScienceFolder = Get-MgDriveRootChild -DriveId $drive.Id | Where-Object Name -eq 'Data Science'

# Get the 'Employee In Office Days' subfolder from the Data Science folder
$employeeFolder = Get-MgDriveItemChild -DriveId $drive.Id -DriveItemId $dataScienceFolder.Id | Where-Object Name -eq 'Employee In Office Days'


# Retrieve the file object for 'leave_data.xlsx'
$file = Get-MgDriveItemChild -DriveId $drive.Id -DriveItemId $employeeFolder.Id | Where-Object Name -eq 'leave_data.xlsx'

# Download the file to the local path
$destinationPath = "data\leave_data.xlsx"
Invoke-MgGraphRequest -Method GET `
  -Uri "https://graph.microsoft.com/v1.0/drives/$($drive.Id)/items/$($file.Id)/content" `
  -OutputFilePath $destinationPath
