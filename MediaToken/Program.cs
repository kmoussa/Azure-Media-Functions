using System;

using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.MediaServices.Client;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Amazon.S3;
using Amazon;
using Amazon.S3.Model;
using System.IO;
using Microsoft.WindowsAzure.MediaServices;
using AWSDBModel;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
namespace MediaToken
{
    class Program
    {
        static CloudMediaContext _context;
        static string _appkey = "AKIAI3FVLNOIXNSYFU2Q";
        static string _appsecret = "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";
        static string _mbcappkey = "AKIAJW2L6IS7KALBGFOA";
        static string _mbcappsecret = "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";
        static string serviceurl = "https://s3-eu-central-1.amazonaws.com";
        static string domain = "mbcsp.onmicrosoft.com";
        static string serviceprincipalId = "9ed4ed2f-5437-4cb6-a186-2d927ac71d52";
        static string servicekey = "Iku6GPX0HfLL4HqOLl3U2uz2CiVWJlDTCpHlW7TuZvQ=" ;
        static string restendpoint = "https://shahiddotnet.restv2.westeurope.media.azure.net/api/";
        static string storagename = "shahid02";
        static string storagekey = "8cx4vOpTF3FoCVxjGlsSbBlZIubtUPFxSCsXiWO2zD4Qq+6/TlxEFs1MnwBxU7u6A4Q8e2V560iAHSssWT1QwQ==" ;
        static void Main(string[] args)
        {

             MainAsync(args).GetAwaiter().GetResult();
            _context = AzureServicePrincipalAuth();

            // await MigrateAsync("AKIAI3FVLNOIXNSYFU2Q", "FfHHxSShXD/vlrxY1yeaOJfx+mAqHqFOAGZVxIJe");
            //Migrate("AKIAJW2L6IS7KALBGFOA", "Qqu/HH0ysILKZ2wXVmi0hb+kBMqY//MMKo8L/wFh");


        }

         static async Task MainAsync(string[] args)
        {
            _context = AzureServicePrincipalAuth();

            await MigrateAsync(_appkey, _appsecret);
           // await MigrateMBCAsync(_mbcappkey, _mbcappsecret);
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

        static async Task MigrateAsync(string accesskey, string secretkey)
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


                    await GetAssetURLAsync(s3Client, b, o);

                }
            }


        }

        static async Task MigrateMBCAsync(string accesskey, string secretkey)
        {
            string accessKey = accesskey;
            string secretKey = secretkey;
            int count = 0;
            AmazonS3Config config = new AmazonS3Config();
            config.ServiceURL = "https://s3-eu-central-1.amazonaws.com";

            AmazonS3Client s3Client = new AmazonS3Client(
                    accessKey,
                    secretKey,
                    config
                    );
            
            string url = string.Empty;
            ListBucketsResponse bresponse = s3Client.ListBuckets();
            foreach (S3Bucket b in bresponse.Buckets)
            {
                if (b.BucketName == "shahid-deluxe")
                {
                    ListObjectsV2Request request = new ListObjectsV2Request
                    {
                        BucketName = b.BucketName,
                        Prefix = "upload/"
                    };

                    ListObjectsV2Response oresponse;
                     do
                    {
                        oresponse = s3Client.ListObjectsV2(request);
                        foreach (S3Object o in oresponse.S3Objects)
                        {
                            count++;
                            
                           await GetAssetURLAsync(s3Client, b, o);
                        }
                        
                        request.ContinuationToken = oresponse.NextContinuationToken;
                    } while (oresponse.IsTruncated == true);
                    
                }
            }

          
            // Fetching the name from the path parameter in the request URL

        }

        static async Task GetAssetURLAsync(AmazonS3Client s3Client, S3Bucket b, S3Object so)
        {
            try
            {
                GetPreSignedUrlRequest srequest = new GetPreSignedUrlRequest();
                srequest.BucketName = b.BucketName;
                srequest.Key = so.Key;
                srequest.Expires = DateTime.Now.AddHours(1);
                srequest.Protocol = Protocol.HTTP;
                string url =s3Client.GetPreSignedURL(srequest);
                UriBuilder u = new UriBuilder(url);
                await CopyAssetToAzureAsync(_context, u.Uri, so.Key, so.Key, "devoss", "BKcQivNdW19t/L53aheZJsfUeq5rxXt2TSdsrv7IXxzK4Ftt2dV1b8yo/0hnmSl5jK/9g5xfaO6ca6uLL0snug==", AssetCreationOptions.None);
                
            }
            catch (AmazonS3Exception ex)
            {

                throw ex;
            }

        }

        #region not needed
        //static void AzureADAuth()
        //{
        //    var tokenCredentials = new AzureAdTokenCredentials("fabrikamgulf.com",
        //        new AzureAdClientSymmetricKey("9ed4ed2f-5437-4cb6-a186-2d927ac71d52", "Iku6GPX0HfLL4HqOLl3U2uz2CiVWJlDTCpHlW7TuZvQ="),
        //        AzureEnvironments.AzureCloudEnvironment);
        //    var tokenProvider = new AzureAdTokenProvider(tokenCredentials);

        //    CloudMediaContext _context = new CloudMediaContext(new Uri("https://devoss.restv2.westeurope.media.azure.net/api/"), tokenProvider);

        //    var assets = _context.Assets;
        //    foreach (var a in assets)
        //    {
        //        Console.WriteLine(a.Name);
        //    }
        //}
        //static MemoryStream getAWSAsset(AmazonS3Client s3Client, S3Object o, S3Bucket b)
        //{
        //    using (s3Client)
        //    {
        //        MemoryStream file = new MemoryStream();
        //        try
        //        {
        //            GetObjectResponse r = s3Client.GetObject(new GetObjectRequest()
        //            {
        //                BucketName = b.BucketName,
        //                Key = o.Key
        //            });
        //            try
        //            {
        //                long transferred = 0L;
        //                BufferedStream stream2 = new BufferedStream(r.ResponseStream);
        //                byte[] buffer = new byte[0x2000];
        //                int count = 0;
        //                while ((count = stream2.Read(buffer, 0, buffer.Length)) > 0)
        //                {
        //                    file.Write(buffer, 0, count);
        //                }
        //            }
        //            finally
        //            {
        //            }
        //            return file;
        //        }
        //        catch (AmazonS3Exception)
        //        {
        //            return null;
        //            //Show exception
        //        }
        //    }
        //}

        //static void deleteAsset(AmazonS3Client s3Client, S3Bucket b, S3Object o)
        //{
        //    DeleteObjectRequest request = new DeleteObjectRequest()
        //    {
        //        BucketName = b.BucketName,
        //        Key = o.Key
        //    };
        //    DeleteObjectResponse response = s3Client.DeleteObject(request);
        //}
        #endregion

        static async Task CopyAssetToAzureAsync(CloudMediaContext _context, Uri ObjectUrl, string assetname, string fileName,string targetStorage,string targetStorageKey,AssetCreationOptions o)
        {
            AWSDBModel.medialogdab db = new medialogdab();
            log l = new log();
            CloudBlockBlob blockBlob;
            IAssetFile assetFile;
            IAsset asset;
            ILocator destinationLocator = null;
            IAccessPolicy writePolicy = null;


            CloudStorageAccount storageAccount = new CloudStorageAccount(new StorageCredentials(targetStorage, targetStorageKey), true);
            CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();


            asset = _context.Assets.Create(assetname, targetStorage,o);
            writePolicy = _context.AccessPolicies.Create("writePolicy", TimeSpan.FromDays(2), AccessPermissions.Write);
            assetFile = asset.AssetFiles.Create(fileName);
            destinationLocator = _context.Locators.CreateLocator(LocatorType.Sas, asset, writePolicy);
            Uri uploadUri = new Uri(destinationLocator.Path);
            string assetContainerName = uploadUri.Segments[1];

            CloudBlobContainer mediaBlobContainer = cloudBlobClient.GetContainerReference(assetContainerName);

            mediaBlobContainer.CreateIfNotExists();

            blockBlob = mediaBlobContainer.GetBlockBlobReference(fileName);

            blockBlob = mediaBlobContainer.GetBlockBlobReference(fileName);
            await blockBlob.StartCopyFromBlobAsync(ObjectUrl);
            
                l.AWS_Asset = ObjectUrl.ToString();
                l.AWS_bucket = "";
                l.Azure_Asset = blockBlob.ToString();
                l.blobstorageURI = blockBlob.Uri.ToString();
                l.AssetId = assetFile.Id;
                l.Filename = fileName;
                l.CopyStartdate = DateTime.Now;
                l.CopyStatus = false;
                l.DestContainer = mediaBlobContainer.Name;
                l.Encoded_ = false;
                l.Encrypted = false;
                l.MediaUpdateStatus = false;

                db.logs.Add(l);
                db.SaveChanges();
            
            //SetFileAsPrimary(asset, assetFile.Name);
        }
        static public void SetFileAsPrimary(IAsset asset, string assetfilename)
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
                }
                catch
                {
                    throw;
                }
            }
        }
    }
}
