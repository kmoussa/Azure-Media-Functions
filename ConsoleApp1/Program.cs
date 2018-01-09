using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon;
using Amazon.S3.Model;
using System.IO;
using Microsoft.WindowsAzure.MediaServices;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.MediaServices.Client;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using System.Threading;

namespace ConsoleApp1
{
    class Program
    {

        static CloudMediaContext _context;

        static void Main(string[] args)
        {

            migrate("AKIAI3FVLNOIXNSYFU2Q", "FfHHxSShXD/vlrxY1yeaOJfx+mAqHqFOAGZVxIJe");
        }

        static void migrate(string accesskey, string secretkey)
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


                    url = getAssetURL(s3Client, b, o);
                    Console.WriteLine(url);
                }
            }
            Console.ReadLine();

            // Fetching the name from the path parameter in the request URL

        }
        static string getAssetURL(AmazonS3Client s3Client, S3Bucket b, S3Object so)
        {
            try
            {
                GetPreSignedUrlRequest srequest = new GetPreSignedUrlRequest();
                srequest.BucketName = b.BucketName;
                srequest.Key = so.Key;
                srequest.Expires = DateTime.Now.AddHours(1);
                srequest.Protocol = Protocol.HTTP;
                string url = s3Client.GetPreSignedURL(srequest);
                return url;

            }
            catch (AmazonS3Exception ex)
            {

                throw ex;
            }

        }
        static public void checkstatus(string targetStorage, string targetStorageKey)
        {
            CloudStorageAccount storageAccount = new CloudStorageAccount(new StorageCredentials(targetStorage, targetStorageKey), true);
            CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

            bool copyStatus = true;
            var AllBlobs = cloudBlobClient.ListBlobs(null, true, BlobListingDetails.Copy);
            foreach (var dest in AllBlobs)
            {
             
                var blockBlob = dest as CloudBlockBlob;
                if (blockBlob.CopyState.Status == CopyStatus.Success)
                {
                    asset = _context.Assets.Where(a => a.Id == asset.Id).FirstOrDefault();
                    blockBlob.FetchAttributes();
                    assetFile.ContentFileSize = blockBlob.Properties.Length;
                    assetFile.Update();
                    destinationLocator.Delete();
                    writePolicy.Delete();
                    // Refresh the asset.
                  

                    // make the file primary
                    SetFileAsPrimary(asset, assetFile.Name);
                }
            

           
            }
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
