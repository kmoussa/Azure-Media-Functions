using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.MediaServices.Client;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
//using AWSDBModel;
using System.Configuration;
using System.Data.SqlClient;
namespace AWS_Media
{

    public static class migrate
    {
        static CloudMediaContext _context = null;
        static CloudBlockBlob blockBlob = null;
        static IAssetFile assetFile;
        static IAsset asset;
        static ILocator destinationLocator = null;
        static IAccessPolicy writePolicy = null;
        static CloudBlobClient cloudBlobClient;
        static bool allbloblareupdtodate;
        [FunctionName("migrate")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "")]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("Migration started");
            _context = AzureServicePrincipalAuth();

            await Migrate("AKIAI3FVLNOIXNSYFU2Q", "FfHHxSShXD/vlrxY1yeaOJfx+mAqHqFOAGZVxIJe");


            // Fetching the name from the path parameter in the request URL
            return req.CreateResponse(HttpStatusCode.OK);
        }
        static CloudMediaContext AzureServicePrincipalAuth()
        {
            
            var tokenCredentials = new AzureAdTokenCredentials("fabrikamgulf.com",
                            new AzureAdClientSymmetricKey("0be4cffd-1aa5-4be5-9082-59a8be333edb", "ysA4gg23HHopCXkUlUU+wS1H6AbBsuDn1/7glzqEp7o="),
                            AzureEnvironments.AzureCloudEnvironment);

            var tokenProvider = new AzureAdTokenProvider(tokenCredentials);

            CloudMediaContext _context = new CloudMediaContext(new Uri("https://devoss.restv2.westeurope.media.azure.net/api/"), tokenProvider);
            return _context;
        }

        static async Task Migrate(string accesskey, string secretkey)
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


                    await getAssetURLAsync(s3Client, b, o);

                }
            }


        }

        static void AzureADAuth()
        {
            var tokenCredentials = new AzureAdTokenCredentials("fabrikamgulf.com", AzureEnvironments.AzureCloudEnvironment);
            var tokenProvider = new AzureAdTokenProvider(tokenCredentials);

            CloudMediaContext _context = new CloudMediaContext(new Uri("https://devoss.restv2.westeurope.media.azure.net/api/"), tokenProvider);
        }

        static async Task getAssetURLAsync(AmazonS3Client s3Client, S3Bucket b, S3Object so)
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
                await CopyAssetToAzureAsync(_context, u.Uri, so.Key, so.Key, "devoss", "BKcQivNdW19t/L53aheZJsfUeq5rxXt2TSdsrv7IXxzK4Ftt2dV1b8yo/0hnmSl5jK/9g5xfaO6ca6uLL0snug==", AssetCreationOptions.None);

            }
            catch (AmazonS3Exception ex)
            {

                throw ex;
            }

        }


        #region not needed
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
        #endregion

        static async Task CopyAssetToAzureAsync(CloudMediaContext _context, Uri ObjectUrl, string assetname, string fileName, string targetStorage, string targetStorageKey, AssetCreationOptions o)
        {

           //AWSDBModel.medialogdab db = new AWSDBModel.medialogdab();
            
           // log l = new log();
            CloudStorageAccount storageAccount = new CloudStorageAccount(new StorageCredentials(targetStorage, targetStorageKey), true);
            cloudBlobClient = storageAccount.CreateCloudBlobClient();
       
            asset = _context.Assets.Create(assetname, targetStorage, o);
            writePolicy = _context.AccessPolicies.Create("writePolicy", TimeSpan.FromDays(2), AccessPermissions.Write);
            assetFile = asset.AssetFiles.Create(fileName);
            destinationLocator = _context.Locators.CreateLocator(LocatorType.Sas, asset, writePolicy);
            Uri uploadUri = new Uri(destinationLocator.Path);
            string assetContainerName = uploadUri.Segments[1];

            CloudBlobContainer mediaBlobContainer = cloudBlobClient.GetContainerReference(assetContainerName);


            if (mediaBlobContainer.CreateIfNotExists())
            {
                mediaBlobContainer.SetPermissions(new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Blob
                });
            }

            blockBlob = mediaBlobContainer.GetBlockBlobReference(fileName);
            if (await StartCopy(ObjectUrl) == true)
            {
                //l.AWS_Asset = ObjectUrl.ToString();
                //l.AWS_bucket = "";
                //l.Azure_Asset = blockBlob.ToString();
                //l.blobstorageURI = blockBlob.Uri.ToString();
                //l.AssetId = assetFile.Id;
                //l.Filename = fileName;
                //l.CopyStartdate = DateTime.Now;
                //l.CopyStatus = false;
                //l.DestContainer = mediaBlobContainer.Name;
                //l.Encoded_ = false;
                //l.Encrypted = false;
                //l.MediaUpdateStatus = false;
               
                //db.logs.Add(l);
                //db.SaveChanges();
            };
            
        }
        static async Task<bool> StartCopy(Uri ObjectUrl)
        {
            await blockBlob.StartCopyAsync(ObjectUrl);

            if (checkstatus(blockBlob))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        static public bool checkstatus(CloudBlockBlob blockBlob)
        {

            allbloblareupdtodate = true;
            IEnumerable<CloudBlobContainer> containers = cloudBlobClient.ListContainers();
            do
            {
                Thread.Sleep(5000);
                foreach (var item in containers)
                {
                    foreach (var blob in item.ListBlobs())
                    {
                        ICloudBlob destblob = cloudBlobClient.GetBlobReferenceFromServer(blob.StorageUri, null, null, null);
                        if (destblob.CopyState != null)
                        {
                            if (destblob.CopyState.Status == CopyStatus.Success)
                            {
                                destblob.FetchAttributes();
                                IAsset file = _context.Assets.Where(x => x.Name == destblob.Name).FirstOrDefault();
                                IAssetFile afile = file.AssetFiles.FirstOrDefault();


                                if (afile.ContentFileSize <= 0)
                                {
                                    afile.ContentFileSize = destblob.Properties.Length;
                                    afile.Update();
                                    destinationLocator.Delete();
                                    writePolicy.Delete();
                                    // Refresh the asset.
                                    asset = _context.Assets.Where(a => a.Id == afile.Id).FirstOrDefault();
                                    if (asset != null)
                                    {
                                        // make the file primary
                                        SetFileAsPrimary(asset, afile.Name);

                                    }
                                }

                            }

                        }

                    }

                }

            } while (!allbloblareupdtodate);
             return true;
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