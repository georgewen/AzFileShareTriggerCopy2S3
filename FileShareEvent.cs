using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Amazon.S3;
using System.IO;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using System.Reflection.Metadata;
using Amazon.S3.Transfer;

namespace Fileshare_Trigger
{
    public static class FileShareEvent
    {
        [FunctionName("FileShareTrigger")]
        public static async Task Run(
            [EventHubTrigger("fileshare-write-event", 
            Connection = "EVENT_HUB_CONNECTION_STRING")] EventData[] events, 
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    // Process message here
                    await Task.Yield();
                    string targetPath = Environment.GetEnvironmentVariable("TARGET_DIRECTORY");
                    LogStream logs = JsonConvert.DeserializeObject<LogStream>(eventData.EventBody.ToString());
                    foreach (Record record in logs.records)
                    {
                        // Only process for file that is completed
                        if (record.operationName == "PutRange")
                        {
                            string url = record.uri;
                            Uri uri = new Uri(url);
                            string filePath = uri.AbsolutePath; ///myfilesharetest/upload/yob2008.txt

                            if (targetPath != null && filePath.Contains(targetPath))
                            {
                                // Target path is defined
                                await UploadFileToS3(filePath, log);
                            }
                            else if(targetPath == null)
                            {
                                // No target path defined, so all files will be sent out.
                                await UploadFileToS3(filePath, log);
                            }
                        }
                    };
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        public static async Task UploadFileToS3(string filePath, ILogger log)
        {
            LogicAppBody body = new LogicAppBody()
            {
                path = filePath
            };

            log.LogInformation($"Request sent for {filePath}.");


            //copy file from file share to s3

            string connectionString = Environment.GetEnvironmentVariable("AZ_STORAGE_CONNECTION_STRING"); //"DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=<account-key>";

            // Name of the share, directory, and file we'll download from
            string shareName = "myfilesharetest";
            string dirName = "upload";
            string fileName = Path.GetFileName(filePath); ;

            string bucketName = "doc-example-bucket";

            string AccessKey = "";
            string SecretKey = "";
            string AWS_Region = "";

            // Get a reference to the file
            ShareClient share = new ShareClient(connectionString, shareName);
            ShareDirectoryClient directory = share.GetDirectoryClient(dirName);
            ShareFileClient file = directory.GetFileClient(fileName);

            // Download the file
            ShareFileDownloadInfo download = file.Download();


            AmazonS3Client s3Client = new AmazonS3Client(AccessKey, SecretKey, AWS_Region);
            using (var ms = new MemoryStream())
            {
                // Download blob content to stream
                download.Content.CopyTo(ms);

                var uploadRequest = new TransferUtilityUploadRequest
                {
                    InputStream = ms,
                    Key = fileName,
                    BucketName = bucketName,
                  //  ContentType = file.ContentType
                };

                var fileTransferUtility = new TransferUtility(s3Client);
                await fileTransferUtility.UploadAsync(uploadRequest);
            }
        }
    }
}
