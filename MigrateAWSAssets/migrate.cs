
using System.Net;
using System.Net.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Amazon.S3;
using Amazon.S3.Model;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.MediaServices.Client;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System;

namespace MigrateAWSAssets
{
    public static class migrate
    {

      
        [FunctionName("migrate")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "")]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("The Migrate operation has started.");


          

            //await Migrate("AKIAI3FVLNOIXNSYFU2Q", "FfHHxSShXD/vlrxY1yeaOJfx+mAqHqFOAGZVxIJe");
       

            // Fetching the name from the path parameter in the request URL
            return req.CreateResponse(HttpStatusCode.OK);
        }

        static void GetS3Assets(string accesskey, string secretkey)
        {
            string accessKey = accesskey;
            string secretKey = secretkey;

            AmazonS3Config config = new AmazonS3Config();
            config.ServiceURL = "https://s3-eu-west-1.amazonaws.com";

            AmazonS3Client s3Client = new AmazonS3Client(
                    accessKey,
                    secretKey,
                    config
                    );
            string url = string.Empty;
            ListBucketsResponse bresponse = s3Client.ListBuckets();
            foreach (S3Bucket b in bresponse.Buckets)
            {
                ListObjectsRequest request = new ListObjectsRequest();
                request.BucketName = b.BucketName;
                ListObjectsResponse oresponse = s3Client.ListObjects(request);
                foreach (S3Object o in oresponse.S3Objects)
                {

                    getAssetURLAsync(s3Client, b, o);

                }
            }

        }


        static Uri getAssetURLAsync(AmazonS3Client s3Client, S3Bucket b, S3Object so)
        {
            try
            {
                GetPreSignedUrlRequest srequest = new GetPreSignedUrlRequest();
                srequest.BucketName = b.BucketName;
                srequest.Key = so.Key;
                srequest.Expires = DateTime.Now.AddHours(1);
                srequest.Protocol = Protocol.HTTP;
                string url = s3Client.GetPreSignedURL(srequest);
                UriBuilder u = new UriBuilder(url);
                return u.Uri;
                
            }
            catch (AmazonS3Exception ex)
            {

                throw ex;
            }

        }

        static MemoryStream getAWSAsset(AmazonS3Client s3Client, S3Object o, S3Bucket b)
        {
            using (s3Client)
            {
                MemoryStream file = new MemoryStream();
                try
                {
                    GetObjectResponse r = s3Client.GetObject(new GetObjectRequest()
                    {
                        BucketName = b.BucketName,
                        Key = o.Key
                    });
                    try
                    {
                        long transferred = 0L;
                        BufferedStream stream2 = new BufferedStream(r.ResponseStream);
                        byte[] buffer = new byte[0x2000];
                        int count = 0;
                        while ((count = stream2.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            file.Write(buffer, 0, count);
                        }
                    }
                    finally
                    {
                    }
                    return file;
                }
                catch (AmazonS3Exception)
                {
                    return null;
                    //Show exception
                }
            }
        }

        static void deleteAsset(AmazonS3Client s3Client, S3Bucket b, S3Object o)
        {
            DeleteObjectRequest request = new DeleteObjectRequest()
            {
                BucketName = b.BucketName,
                Key = o.Key
            };
            DeleteObjectResponse response = s3Client.DeleteObject(request);
        }
        static async Task CopyAssetToAzureAsync(CloudMediaContext _context, Uri ObjectUrl, string assetname, string fileName, string targetStorage, string targetStorageKey, AssetCreationOptions o)
        {
            CloudBlockBlob blockBlob;
            IAssetFile assetFile;
            IAsset asset;
            ILocator destinationLocator = null;
            IAccessPolicy writePolicy = null;


            CloudStorageAccount storageAccount = new CloudStorageAccount(new StorageCredentials(targetStorage, targetStorageKey), true);
            CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();


            asset = _context.Assets.Create(assetname, targetStorage, o);
            writePolicy = _context.AccessPolicies.Create("writePolicy", TimeSpan.FromDays(2), AccessPermissions.Write);
            assetFile = asset.AssetFiles.Create(fileName);
            destinationLocator = _context.Locators.CreateLocator(LocatorType.Sas, asset, writePolicy);
            Uri uploadUri = new Uri(destinationLocator.Path);
            string assetContainerName = uploadUri.Segments[1];

            CloudBlobContainer mediaBlobContainer = cloudBlobClient.GetContainerReference(assetContainerName);

            mediaBlobContainer.CreateIfNotExists();

            blockBlob = mediaBlobContainer.GetBlockBlobReference(fileName);

            string stringOperation = await blockBlob.StartCopyAsync(ObjectUrl);

            
        }
        static public bool SetFileAsPrimary(IAsset asset, string assetfilename)
        {
            var ismAssetFiles = asset.AssetFiles.ToList().
                Where(f => f.Name.Equals(assetfilename, StringComparison.OrdinalIgnoreCase)).ToArray();

            if (ismAssetFiles.Count() == 1)
            {
                try
                {
                    // let's remove primary attribute to another file if any
                    asset.AssetFiles.Where(af => af.IsPrimary).ToList().ForEach(af => { af.IsPrimary = false; af.Update(); });
                    ismAssetFiles.First().IsPrimary = true;
                    ismAssetFiles.First().Update();
                    return true;
                }
                catch
                {
                    return false;
                }
            }
            return false;
        }
    }
}
