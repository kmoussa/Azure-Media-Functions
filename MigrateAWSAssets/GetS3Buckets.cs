using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

namespace MigrateAWSAssets
{
    public static class GetS3Buckets
    {
        // Read values from the App.config file.
        static string accessKey = Environment.GetEnvironmentVariable("AWSS3AccessKeyTest");
        static string secretKey = Environment.GetEnvironmentVariable("AWSS3SecretKeyTest");
        static string ServiceUrl = Environment.GetEnvironmentVariable("AWSS3ServiceURLTest");
        [FunctionName("GetS3Buckets")]
        public static HttpResponseMessage Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "GetS3Buckets")]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");
            AmazonS3Config config = new AmazonS3Config();
            config.ServiceURL = ServiceUrl;

            AmazonS3Client s3Client = new AmazonS3Client(
                    accessKey,
                    secretKey,
                    config
                    );

            string url = string.Empty;
            ListBucketsResponse bresponse = s3Client.ListBuckets();


          
            log.Info("Found " + bresponse.Buckets.Count + " of buckets.");

            return req.CreateResponse(HttpStatusCode.OK,bresponse);
        }
    }
}
