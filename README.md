# Azure-Media-Functions

Some Azure Functions to help customers integrate with Azure Media Services

<b>- GetAssetUrl</b>
This function help generate an S3 asset Url in order to pass it along to a standard upload

<b>- GetS3AssetsfromBucket</b>
This function helps retreiving S3 assets from S3 Bucket where we should use a foreach loop in Azure logic apps to pass them to GetAssetUrl Function.

<b>- GetS3Bucket</b>
This function helps retreiving an S3 Buckets in where we should use a foreach loop in Azure logic apps to pass them to GetS3AssetsfromBucket Function.

<b>- GetTelemetryToSQL</b>
This function migrates Azure Media services telemetry from Azure Storage to Azure SQL DB, you can alter the code to copy the data to any database of your choice.

I built a simple Azure Logic App with a recurrence trigger for that runs every min. to trigger the GetTelemetryToSQL function- the first time it runs I suggest that you run the function manually since it might take longer to migrate the data if you have tons of it, otherwise it will migrate all the data from all tables and the next time it runs it will pick up from where it stopped. 

<b>- UpdateAlternativeId</b>
This function help you update alternativeId for an Asset in Azure Media Services.

<b>- UploadFromLimeLight</b>
This function helps migrating assets from LimeLight to Azure Media Services, We need to integrate it with Azure media functions under this <a href='https://github.com/xpouyat/media-services-dotnet-functions-integration/tree/master/media-functions-for-logic-app'>git rep</a>.
