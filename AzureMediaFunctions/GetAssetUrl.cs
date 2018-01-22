using System;
using System.Net;
using System.Net.Http;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.MediaServices.Client;

namespace AzureMediaFunctions
{
    public static class GetAssetUrl
    {
        // Read values from the App.config file.
        static string accessKey = Environment.GetEnvironmentVariable("AWSS3AccessKeyTest");
        static string secretKey = Environment.GetEnvironmentVariable("AWSS3SecretKeyTest");
        static string ServiceUrl = Environment.GetEnvironmentVariable("AWSS3ServiceURLTest");
        [FunctionName("GetAssetUrl")]
        public static HttpResponseMessage Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "GetAssetUrl/info/{bucket}/{s3asset}")]HttpRequestMessage req, string s3asset,string bucket, TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");
         
          

                AmazonS3Config config = new AmazonS3Config();
                config.ServiceURL = ServiceUrl;

                AmazonS3Client s3Client = new AmazonS3Client(
                        accessKey,
                        secretKey,
                        config
                        );
      

                try
                {
                    GetPreSignedUrlRequest srequest = new GetPreSignedUrlRequest();
                    srequest.BucketName = bucket;
                    srequest.Key = s3asset;
                    srequest.Expires = DateTime.Now.AddHours(10);
                    srequest.Protocol = Protocol.HTTP;
                    string url = s3Client.GetPreSignedURL(srequest);
                    UriBuilder u = new UriBuilder(url);
                return req.CreateResponse(HttpStatusCode.OK, u.Uri);
            }
                catch (AmazonS3Exception ex)
                {
                return req.CreateErrorResponse(HttpStatusCode.ExpectationFailed, ex);
                    
                }


            }
  
       
           
        }   
    }

