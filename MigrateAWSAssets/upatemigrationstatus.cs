using System;
using System.Net;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.MediaServices.Client;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using System.Net.Http;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

namespace MigrateAWSAssets
{
    public static class upatemigrationstatus
    {
        // Read values from the App.config file.
        private static readonly string _storageAccountName = Environment.GetEnvironmentVariable("MediaServicesStorageAccountName");
        private static readonly string _storageAccountKey = Environment.GetEnvironmentVariable("MediaServicesStorageAccountKey");

        // Field for service context.
        private static CloudMediaContext _context = null;
        private static CloudStorageAccount _destinationStorageAccount = null;
        [FunctionName("upatemigrationstatus")]
        public static async Task<object> Run(HttpRequestMessage req, TraceWriter log)
        {
        
            string jsonContent = await req.Content.ReadAsStringAsync();
            dynamic data = JsonConvert.DeserializeObject(jsonContent);
            log.Info("Request : " + jsonContent);

            // Validate input objects
            int delay = 5000;
            if (data.targetStorageAccountName == null)
                return req.CreateResponse(HttpStatusCode.BadRequest, new { error = "Please pass targetStorageAccountName in the input object" });
            if (data.targetStorageAccountKey == null)
                return req.CreateResponse(HttpStatusCode.BadRequest, new { error = "Please pass targetStorageAccountKey in the input object" });
            if (data.targetContainer == null)
                return req.CreateResponse(HttpStatusCode.BadRequest, new { error = "Please pass targetContainer in the input object" });
            if (data.delay != null)
                delay = data.delay;
            log.Info("Input - DestinationContainer : " + data.targetContainer);
            //log.Info("delay : " + delay);

            log.Info($"Wait " + delay + "(ms)");
            System.Threading.Thread.Sleep(delay);

            string targetStorageAccountName = data.targetStorageAccountName;
            string targetStorageAccountKey = data.targetStorageAccountKey;
            string targetContainer = data.targetContainer;


            CopyStatus copyStatus = CopyStatus.Success;
            try
            {

                CloudBlobContainer destinationBlobContainer = GetCloudBlobContainer(targetStorageAccountName, targetStorageAccountKey, targetContainer);

                string blobPrefix = null;

                if (data.fileName != null)
                {
                    blobPrefix = (string)data.fileName;
                    log.Info($"{blobPrefix}");
                }

                bool useFlatBlobListing = true;
                var destBlobList = destinationBlobContainer.ListBlobs(blobPrefix, useFlatBlobListing, BlobListingDetails.Copy);
                foreach (var dest in destBlobList)
                {
                    var destBlob = dest as CloudBlob;
                    if (destBlob.CopyState.Status == CopyStatus.Aborted || destBlob.CopyState.Status == CopyStatus.Failed)
                    {
                        // Log the copy status description for diagnostics and restart copy
                        destBlob.StartCopyAsync(destBlob.CopyState.Source);
                        copyStatus = CopyStatus.Pending;
                    }
                    else if (destBlob.CopyState.Status == CopyStatus.Pending)
                    {
                        // We need to continue waiting for this pending copy
                        // However, let us log copy state for diagnostics
                        copyStatus = CopyStatus.Pending;
                    }
                    // else we completed this pending copy
                }
            }
            catch (Exception ex)
            {
                log.Info("Exception " + ex);
                return req.CreateResponse(HttpStatusCode.BadRequest);
            }

            return req.CreateResponse(HttpStatusCode.OK, new
            {
                copyStatus = copyStatus,
                isRunning = (copyStatus == CopyStatus.Pending).ToString(),
                isSuccessful = (copyStatus == CopyStatus.Success).ToString()
            });
        }
        static public CloudBlobContainer GetCloudBlobContainer(string storageAccountName, string storageAccountKey, string containerName)
        {
            CloudStorageAccount sourceStorageAccount = new CloudStorageAccount(new StorageCredentials(storageAccountName, storageAccountKey), true);
            CloudBlobClient sourceCloudBlobClient = sourceStorageAccount.CreateCloudBlobClient();
            return sourceCloudBlobClient.GetContainerReference(containerName);
        }
    }

}

