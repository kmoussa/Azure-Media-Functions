using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.MediaServices.Client;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace AzureMediaFunctions
{
    public static class UploadFromLimelight
    {
       

        private static CloudMediaContext _context;
        private static string domain = Environment.GetEnvironmentVariable("domain");
        private static string restendpoint = Environment.GetEnvironmentVariable("restendpoint");
        private static string servicekey = Environment.GetEnvironmentVariable("servicekey");
        private static string serviceprincipaleId = Environment.GetEnvironmentVariable("serviceprincipleId");
        private static string storagekey = Environment.GetEnvironmentVariable("storagekey");
        private static string storagename = Environment.GetEnvironmentVariable("storagename");

        [FunctionName("UploadFromLimelight")]
        public static async System.Threading.Tasks.Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "")]HttpRequestMessage req, TraceWriter log)
        {
            _context = AzureServicePrincipalAuth();
            log.Info("C# HTTP trigger function processed a request.");
            string jsonContent = await req.Content.ReadAsStringAsync();
            dynamic data = JsonConvert.DeserializeObject(jsonContent);

            log.Info(jsonContent);
            string url = data.url;
            string filename = data.file_name;
            string shahidmediaid = data.shahid_media_id;
            UriBuilder u = new UriBuilder(url);
            string output =  CopyAssetToAzure(_context, u.Uri, filename, filename, storagename, storagekey, shahidmediaid, AssetCreationOptions.None);
            // Fetching the name from the path parameter in the request URL
            return req.CreateResponse(HttpStatusCode.OK,output);
        }
        private static string CopyAssetToAzure(CloudMediaContext _context, Uri ObjectUrl, string assetname, string fileName, string targetStorage, string targetStorageKey, string alternateId, AssetCreationOptions o)
        {
            IAccessPolicy accessPolicy = null;
            IAsset asset = _context.Assets.Create(assetname, targetStorage, o);
            asset.AlternateId = alternateId;
            IAssetFile file = asset.AssetFiles.Create(fileName);
            file.Asset.AlternateId = alternateId;
            asset.Update();
            //accessPolicy = _context.AccessPolicies.Create(file.Id, TimeSpan.FromDays(2.0), AccessPermissions.Write);
              //string containerName = new Uri(_context.Locators.CreateLocator(LocatorType.Sas, asset, accessPolicy, null, file.Id.ToString()).Path).Segments[1];
            string containerName = "asset-" + asset.Id.Replace("nb:cid:UUID:","");
            CloudBlobContainer containerReference = new CloudStorageAccount(new StorageCredentials(targetStorage, targetStorageKey), true).CreateCloudBlobClient().GetContainerReference(containerName);
            containerReference.CreateIfNotExists(null, null);
            containerReference.GetBlockBlobReference(fileName);
            containerReference.GetBlockBlobReference(fileName).StartCopy(ObjectUrl, null, null, null, null);
            jsonoutput jsonoutput1 = new jsonoutput
            {
                assetid = asset.Id,
                destinationContainer = containerReference.Name
            };
            return JsonConvert.SerializeObject(jsonoutput1);
        }

  
        static CloudMediaContext AzureServicePrincipalAuth()
        {
            var tokenCredentials = new AzureAdTokenCredentials(domain,
                            new AzureAdClientSymmetricKey(serviceprincipaleId, servicekey),
                            AzureEnvironments.AzureCloudEnvironment);

            var tokenProvider = new AzureAdTokenProvider(tokenCredentials);

            CloudMediaContext _context = new CloudMediaContext(new Uri(restendpoint), tokenProvider);
            return _context;
        }

        public class jsonoutput
        {
            // Fields
            public string assetid;
            public string destinationContainer;
        }

    }
}
