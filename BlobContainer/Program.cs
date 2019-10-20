using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace BlobContainer
{
    class Program
    {
        private static readonly string storageConnectionString = ConfigurationManager.AppSetting["AzureBlobConnectionString"];


        static void Main(string[] args)
        {
       
            string storageConnectionString = ConfigurationManager.AppSetting["AzureBlobConnectionString"];
            Console.WriteLine("Hello World!");
            ProcessAsync().GetAwaiter().GetResult();
            Console.ReadKey();
        }
        private static async Task ProcessAsync()
        {
            BlobContinuationToken blobContinuationToken = null;
            CloudStorageAccount storageAccount;
            if (CloudStorageAccount.TryParse(storageConnectionString, out storageAccount))
            {
                // If the connection string is valid, proceed with operations against Blob
                // storage here.
                // ADD OTHER OPERATIONS HERE
            }
            else
            {
                // Otherwise, let the user know that they need to define the environment variable.
                Console.WriteLine(
                    "A connection string has not been defined in the system environment variables. " +
                    "Add an environment variable named 'CONNECT_STR' with your storage " +
                    "connection string as a value.");
                Console.WriteLine("Press any key to exit the application.");
                Console.ReadLine();
            }

            CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference("iotbackend");
            var directory = cloudBlobContainer.GetDirectoryReference("dockan");
            //  CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference("iotbackend");

            var prefix = $"dockan/humidity/2019-01-101";
            var list = cloudBlobContainer.ListBlobs(prefix, useFlatBlobListing: true);
            prefix = $"dockan/humidity/historical.zip";
            var text = GetCSVBlobData(prefix);
            var blockBlob = cloudBlobContainer.GetBlockBlobReference("iotbackend/dockan/humidity/historical.zip");
            // var blockblob =  cloudBlobContainer.GetBlockBlobReference(blobName, DateTimeOffset ? snapshotTime);

            var results1 = directory.ListBlobsSegmented(true, BlobListingDetails.None, 500, null, null, null);

            var blobs = results1.Results;
            // Get the value of the continuation token returned by the listing call.



            //var list = cloudBlobContainer.ListBlobs();
            //var blobs = list.OfType<CloudBlockBlob>()
            //    .Where(b => Path.GetExtension(b.Name).Equals("csv"));
            do
            {
                var results = await cloudBlobContainer.ListBlobsSegmentedAsync(prefix, blobContinuationToken);
                // Get the value of the continuation token returned by the listing call.
                int count = results.Results.Count();

                blobContinuationToken = results.ContinuationToken;
                foreach (IListBlobItem item in results.Results)
                {
                    Console.WriteLine(item.Uri);
                }
            } while (blobContinuationToken != null); // Loop while the                                                                                                                                                                                                                                                                                                                                                        token is not null.
        }
        private static async Task<List<IListBlobItem>> ListBlobsAsync(CloudBlobContainer container)
        {
            BlobContinuationToken continuationToken = null;
            List<IListBlobItem> results = new List<IListBlobItem>();
            do
            {
                bool useFlatBlobListing = true;
                BlobListingDetails blobListingDetails = BlobListingDetails.None;
                int maxBlobsPerRequest = 10;
                var prefix = $"iotbackend";

                var response = await container.ListBlobsSegmentedAsync(prefix, useFlatBlobListing, blobListingDetails, maxBlobsPerRequest, continuationToken, null, null);
                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results);
            }
            while (continuationToken != null);


     
            return results;
        }

        private static async Task<List<IListBlobItem>> ListBlobs(CloudBlobDirectory directory)
        {
            BlobContinuationToken continuationToken = null;
            List<IListBlobItem> results = new List<IListBlobItem>();
            do
            {
                bool useFlatBlobListing = true;
                BlobListingDetails blobListingDetails = BlobListingDetails.None;
                int maxBlobsPerRequest = 10;

                var response = await directory.ListBlobsSegmentedAsync(true, BlobListingDetails.None, 500, null, null, null);
                //var response = await container.ListBlobsSegmentedAsync(null, useFlatBlobListing, blobListingDetails, maxBlobsPerRequest, continuationToken, null, null);
                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results);
            }
            while (continuationToken != null);
            return results;
        }


        static class ConfigurationManager
        {
            public static IConfiguration AppSetting { get; }
            static ConfigurationManager()
            {
                AppSetting = new ConfigurationBuilder()
                        .SetBasePath(Directory.GetCurrentDirectory())
                        .AddJsonFile("appsettings.json")
                        .Build();
            }
        }


        private static string GetCSVBlobData(string filename, string connectionString, string containerName)
        {
            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            // Retrieve reference to a blob named "test.csv"
            CloudBlockBlob blockBlobReference = container.GetBlockBlobReference(filename);

            string text;
            using (var memoryStream = new MemoryStream())
            {
                //downloads blob's content to a stream
                blockBlobReference.DownloadToStream(memoryStream);

                //puts the byte arrays to a string
                text = System.Text.Encoding.UTF8.GetString(memoryStream.ToArray());
            }

            return text;
        }
    }
}
