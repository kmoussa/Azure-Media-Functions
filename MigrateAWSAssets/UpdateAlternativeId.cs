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
using Newtonsoft.Json;

namespace MigrateAWSAssets
{


    public static class UpdateAlternativeId
    {
        // Read values from the App.config file.
        static string _storageAccountName = Environment.GetEnvironmentVariable("MediaServicesStorageAccountName");
        static string _storageAccountKey = Environment.GetEnvironmentVariable("MediaServicesStorageAccountKey");

        static readonly string _AADTenantDomain = Environment.GetEnvironmentVariable("AMSAADTenantDomain");
        static readonly string _RESTAPIEndpoint = Environment.GetEnvironmentVariable("AMSRESTAPIEndpoint");

        static readonly string _mediaservicesClientId = Environment.GetEnvironmentVariable("AMSClientId");
        static readonly string _mediaservicesClientSecret = Environment.GetEnvironmentVariable("AMSClientSecret");

        // Field for service context.
        private static CloudMediaContext _context = null;
        private static CloudStorageAccount _destinationStorageAccount = null;

        [FunctionName("UpdateAlternativeId")]
        public static async Task<object> Run(HttpRequestMessage req, TraceWriter log)
        {
            string jsonContent = await req.Content.ReadAsStringAsync();
            dynamic data = JsonConvert.DeserializeObject(jsonContent);

            log.Info(jsonContent);

            if (data.alternativeId == null)
            {
                // for test
                // data.Path = "/input/WP_20121015_081924Z.mp4";

                return req.CreateResponse(HttpStatusCode.BadRequest, new
                {
                    error = "Please pass alternativeId in the input object"
                });
            }

            string alternativeId = data.alternativeId;
            string assetId = data.assetId;
            log.Info($"Using Azure Media Service Rest API Endpoint : {_RESTAPIEndpoint}");

            IAsset Asset = null;

            try
            {
                AzureAdTokenCredentials tokenCredentials = new AzureAdTokenCredentials(_AADTenantDomain,
                                    new AzureAdClientSymmetricKey(_mediaservicesClientId, _mediaservicesClientSecret),
                                    AzureEnvironments.AzureCloudEnvironment);

                AzureAdTokenProvider tokenProvider = new AzureAdTokenProvider(tokenCredentials);

                _context = new CloudMediaContext(new Uri(_RESTAPIEndpoint), tokenProvider);

                log.Info("Finding Asset ");

                Asset = _context.Assets.Where(x => x.Id == assetId).FirstOrDefault();
                Asset.AlternateId = alternativeId;
                Asset.Update();
                log.Info("asset updated.");

            }
            catch (Exception ex)
            {
                log.Info($"Exception {ex}");
                return req.CreateResponse(HttpStatusCode.InternalServerError, new
                {
                    Error = ex.ToString()
                });
            }

            return req.CreateResponse(HttpStatusCode.OK);
        }
    }
}

