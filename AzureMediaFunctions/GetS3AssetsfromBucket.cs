using System;
using System.Net;
using System.Net.Http;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

namespace AzureMediaFunctions
{
    public static class GetS3AssetsfromBucket
    {
        // Read values from the App.config file.
        static string accessKey = Environment.GetEnvironmentVariable("AWSS3aAccessKey");
        static string secretKey = Environment.GetEnvironmentVariable("AWSS3SecretKey");
        static string ServiceUrl = Environment.GetEnvironmentVariable("AWSServiceURL");
        [FunctionName("GetS3AssetsfromBucket")]
        public static HttpResponseMessage Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "GetS3AssetsfromBucket/name/{bucketname}")]HttpRequestMessage req, string bucketname, TraceWriter log)
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
            //ListBucketsResponse bresponse = s3Client.ListBuckets();
            S3Bucket b = new S3Bucket();
            b.BucketName = bucketname;
            
            ListObjectsV2Request request = new ListObjectsV2Request
            {
                BucketName = bucketname
            };

                ListObjectsV2Response oresponse;
                oresponse = s3Client.ListObjectsV2(request);
                log.Info("Found " + oresponse.S3Objects.Count + " of assets.");
        
            // Fetching the name from the path parameter in the request URL
            return req.CreateResponse(HttpStatusCode.OK,oresponse.S3Objects);
        }
    }
}
