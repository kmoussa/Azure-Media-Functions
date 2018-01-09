CREATE TABLE [dbo].[logs]
(
	[Id] INT NOT NULL PRIMARY KEY, 
    [AWS_Asset] NVARCHAR(MAX) NULL, 
    [AWS_bucket] NVARCHAR(300) NULL, 
    [Azure_Asset] NVARCHAR(MAX) NULL, 
    [DestContainer] NVARCHAR(300) NULL, 
    [Filename] NVARCHAR(300) NULL, 
    [blobstorageURI] NVARCHAR(MAX) NULL, 
    [AssetId] NVARCHAR(300) NULL, 
    [CopyStatus] BIT NULL, 
    [MediaUpdateStatus] BIT NULL, 
    [Encoded?] BIT NULL, 
    [Encrypted] BIT NULL, 
    [CopyStartdate] DATETIME NULL, 
    [MediaUpdatedate] DATETIME NULL
)
