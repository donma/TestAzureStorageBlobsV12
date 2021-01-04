using Azure.Storage.Blobs.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace TestAzureStorageBlobs
{
    class Program
    {
 static string connsctionString = "";
        static async Task Main(string[] args)
        {

            while (true)
            {
                Console.WriteLine("\r\n\r\n--------------------------------------------");
                Console.WriteLine("\r\nDONMA AZURE STORAGE BLOB TEST 2020\r\n\r\n--------------------------------------------");
                Console.WriteLine("1>Add Dir And Upload File.");
                Console.WriteLine("2>Delete File.");
                Console.WriteLine("3>List Files.");
                Console.WriteLine("4>List Dirs.");
                Console.WriteLine("5>Delete Dir.");
                Console.WriteLine("6>Create Container.");
                Console.WriteLine("7>Delete By Etag.");
                Console.WriteLine("8>Get File ETag.");
                Console.WriteLine("9>Create Snapshot.");
                Console.WriteLine("10>Restore From Snapshot.");
                Console.WriteLine("11>Soft Delete And UnDelete.");
                Console.WriteLine("12>Get Public Download Path.");
                Console.WriteLine("13>Add 1W JSON Files.");
                Console.WriteLine("14>Add 1W JSON Files And Add Tags");
                Console.WriteLine("15>Search By Tag");
                Console.WriteLine("16>Add 10W JSON Files");
                Console.WriteLine("17>Get Counts");
                Console.WriteLine("--------------------------------------------\r\n");

                Console.Write(">");
                var ans = Console.ReadLine();

                if (ans == "1")
                {
                    AddDirAndUploadFile();
                }
                if (ans == "2")
                {
                    DeleteFiles();
                }
                if (ans == "3")
                {
                    ListFiles();
                }
                if (ans == "4")
                {
                    ListDirs();
                }

                if (ans == "5")
                {
                    DeleteDirAndSonFiles("images/girls/2020_TMP/");
                }
                if (ans == "6")
                {
                    CreateContainer();
                }

                if (ans == "7")
                {
                    DeleteByETag();
                }

                if (ans == "8")
                {
                    GetBlobFileInfo();
                }


                if (ans == "9")
                {
                    CreateSnapshot();
                }

                if (ans == "10")
                {
                    RestoreFromSnapShot();
                }
                if (ans == "11")
                {
                    SoftDeleteAndRecovery();
                }
                if (ans == "12")
                {
                    GetDownloadLinkWithStamp(DateTime.UtcNow.AddSeconds(15));
                }
                if (ans == "13")
                {
                    Add1WDatas();
                }
                if (ans == "14")
                {
                    Add1WDataAndAddTag();
                }
                if (ans == "15")
                {
                    SearchByTag();
                }
                if (ans == "16")
                {
                    Add10WDatas();
                }
                if (ans == "17")
                {

                    GetDataCount();
                }
            }
        }

        private static async Task ListBlobsFlatListing()

        {
            var blobClient = new Azure.Storage.Blobs.BlobContainerClient(connsctionString, "test1");

            try
            {

                var resultSegment = blobClient.GetBlobsAsync(default, default, "data10w/")
                    .AsPages(default, 5000);
                var count1 = 0;
                Stopwatch stopWatch = new Stopwatch();
                stopWatch.Start();
                // Enumerate the blobs returned for each page.
                await foreach (Azure.Page<BlobItem> blobPage in resultSegment)
                {

                    count1 += blobPage.Values.Count;
                }
                Console.WriteLine(count1 + "--");
                Console.WriteLine(stopWatch.Elapsed);

            }
            catch (Azure.RequestFailedException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }


        static void Add10WDatas()
        {

            var usersJson = new List<string>();
            var usersObject = new List<User>();
            for (var i = 1; i <= 100000; i++)
            {

                var u = new User();
                u.Age = i;
                u.Create = new DateTime(2000, 1, 1).AddDays(i);
                u.Id = "USER" + i;
                u.Name = "USERNAME" + i;

                usersJson.Add(JsonConvert.SerializeObject(u));
                usersObject.Add(u);
            }



            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            Parallel.For(1, 100001,
                 index =>
                 {

                     var blobClient = new Azure.Storage.Blobs.BlobClient(connsctionString, "test1", "data10w/data" + index + ".json");
                     //byte[] byteArray = Encoding.ASCII.GetBytes(contents);
                     MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(usersJson[index - 1]));

                     blobClient.Upload(stream, true);

                 });
            Console.WriteLine(stopWatch.Elapsed);


        }

        static void GetDataCount()
        {
            var blobClient = new Azure.Storage.Blobs.BlobContainerClient(connsctionString, "test1");

            var count1 = 0;
            Stopwatch stopWatch1 = new Stopwatch();
            stopWatch1.Start();
            //     foreach (var blobItem in blobClient.GetBlobs(Azure.Storage.Blobs.Models.BlobTraits.None, Azure.Storage.Blobs.Models.BlobStates.None, "data10w/").AsPages(default, 5000))
            foreach (var blobItem in blobClient.GetBlobs(Azure.Storage.Blobs.Models.BlobTraits.Tags, Azure.Storage.Blobs.Models.BlobStates.None, "data10w/").AsPages(default, 5000))
            {
                count1 += blobItem.Values.Count;

            }
            Console.WriteLine(stopWatch1.Elapsed);
            Console.WriteLine(count1 + "!!");

            GC.Collect();
            var count2 = 0;
            Stopwatch stopWatch2 = new Stopwatch();
            stopWatch2.Start();

            foreach (var blobItem in blobClient.GetBlobs(Azure.Storage.Blobs.Models.BlobTraits.CopyStatus, Azure.Storage.Blobs.Models.BlobStates.None, "data10w/"))
            {
                count2++;

            }
            Console.WriteLine(stopWatch2.Elapsed);
            Console.WriteLine(count2 + "!!");



        }

        static void SearchByTag()
        {

            var blobContainerClient = new Azure.Storage.Blobs.BlobContainerClient(connsctionString, "test1");
            var serviceClient = new Azure.Storage.Blobs.BlobServiceClient(connsctionString);

            /// var queryString = @"""NAME"" = 'USERNAME9999'";
            //  var queryString = @"""AGE"" >= '1995' AND ""AGE"" <= '2000'";

            var queryString = @"""CREATE"" >= '2000-01-05' AND ""CREATE"" <= '2000-01-10'";

            var resultBlobItems = serviceClient.FindBlobsByTags(queryString);

            foreach (var data in resultBlobItems)
            {
                Console.WriteLine(data.BlobName);

            }
        }
        static void Add1WDataAndAddTag()
        {

            var usersJson = new List<string>();
            var usersObject = new List<User>();
            for (var i = 1; i <= 10000; i++)
            {

                var u = new User();
                u.Age = i;
                u.Create = new DateTime(2000, 1, 1).AddDays(i - 1);
                u.Id = "USER" + i;
                u.Name = "USERNAME" + i;

                usersJson.Add(JsonConvert.SerializeObject(u));
                usersObject.Add(u);
            }



            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            Parallel.For(1, 10001,
                 index =>
                 {

                     var blobClient = new Azure.Storage.Blobs.BlobClient(connsctionString, "test1", "data1w-4/data" + index + ".json");
                     //byte[] byteArray = Encoding.ASCII.GetBytes(contents);
                     MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(usersJson[index - 1]));

                     var res = blobClient.Upload(stream, true);

                     Dictionary<string, string> tags = new Dictionary<string, string>
                      {
                          { "NAME",usersObject[index-1].Name},
                          { "AGE", usersObject[index-1].Age.ToString() },
                          { "CREATE", usersObject[index-1].Create.ToString("yyyy-MM-dd") }
                      };

                     blobClient.SetTags(tags);
                 });
            Console.WriteLine(stopWatch.Elapsed);

        }

        static void Add1WDatas()
        {

            var usersJson = new List<string>();
            var usersObject = new List<User>();
            for (var i = 1; i <= 10000; i++)
            {

                var u = new User();
                u.Age = i;
                u.Create = new DateTime(2000, 1, 1).AddDays(i);
                u.Id = "USER" + i;
                u.Name = "USERNAME" + i;

                usersJson.Add(JsonConvert.SerializeObject(u));
                usersObject.Add(u);
            }



            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            Parallel.For(1, 10001,
                 index =>
                 {

                     var blobClient = new Azure.Storage.Blobs.BlobClient(connsctionString, "test1", "data1w-1/data" + index + ".json");
                     //byte[] byteArray = Encoding.ASCII.GetBytes(contents);
                     MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(usersJson[index - 1]));

                     blobClient.Upload(stream, true);

                 });
            Console.WriteLine(stopWatch.Elapsed);

        }

        static void GetDownloadLinkWithStamp(DateTime expireDate)
        {


            var blobClient = new Azure.Storage.Blobs.BlobClient(connsctionString, "test1", "samplex.jpg");


            var sasBuilder = new Azure.Storage.Sas.BlobSasBuilder()
            {
                BlobContainerName = blobClient.BlobContainerName,
                BlobName = blobClient.Name,
                //允許 15 秒鐘
                ExpiresOn = expireDate,
                StartsOn = DateTime.UtcNow
            };

            sasBuilder.SetPermissions(Azure.Storage.Sas.BlobSasPermissions.Read);

            Uri sasUri = blobClient.GenerateSasUri(sasBuilder);
            Console.WriteLine(sasUri.ToString());


        }


        static void SoftDeleteAndRecovery()
        {

            var blobClient = new Azure.Storage.Blobs.BlobClient(connsctionString, "test1", "sample_delete.txt");

            blobClient.DeleteIfExists();

            var blobContainerClient = new Azure.Storage.Blobs.BlobContainerClient(connsctionString, "test1");

            foreach (var blobItem in blobContainerClient.GetBlobs(Azure.Storage.Blobs.Models.BlobTraits.All, Azure.Storage.Blobs.Models.BlobStates.Deleted, "sample_delete.txt"))
            {
                Console.WriteLine("\t" + blobItem.Name + "," + (blobItem.Metadata.ContainsKey("TAG") ? blobItem.Metadata["TAG"] : "NO_TAG") + "," + blobItem.Deleted);

                Azure.Storage.Blobs.BlobClient snapshotBlob = blobContainerClient.GetBlobClient(blobItem.Name);
                snapshotBlob.Undelete();

            }


        }


        static void RestoreFromSnapShot()
        {

            var blobClient = new Azure.Storage.Blobs.BlobContainerClient(connsctionString, "test1");

            foreach (var blobItem in blobClient.GetBlobs(Azure.Storage.Blobs.Models.BlobTraits.All, Azure.Storage.Blobs.Models.BlobStates.Snapshots, "sample.txt"))
            {
                Console.WriteLine("\t" + blobItem.Name + "," + (blobItem.Metadata.ContainsKey("TAG") ? blobItem.Metadata["TAG"] : "NO_TAG") + "," + blobItem.Snapshot);

                //如果判斷 meta : TAG 是 SNAPSHOT_4 就還原它
                if (blobItem.Metadata.ContainsKey("TAG") && blobItem.Metadata["TAG"] == "SNAPSHOT_4" && !string.IsNullOrEmpty(blobItem.Snapshot))
                {
                    Azure.Storage.Blobs.BlobClient snapshotBlob = blobClient.GetBlobClient(blobItem.Name);

                    //還原的網址必須加上 ?snapshot=
                    var snapshot_uri = snapshotBlob.Uri.ToString() + "?snapshot=" + blobItem.Snapshot;


                    var liveBlobItem = new Azure.Storage.Blobs.BlobClient(connsctionString, "test1", "sample.txt");
                    liveBlobItem.StartCopyFromUri(new Uri(snapshot_uri));

                }
            }

        }


        static void CreateSnapshot()
        {

            var blobClient = new Azure.Storage.Blobs.BlobClient(connsctionString, "test1", "sample.txt");
            for (var i = 1; i <= 10; i++)
            {
                var meta = new System.Collections.Generic.Dictionary<string, string>();
                meta.Add("TAG", "SNAPSHOT_" + i);

                var res = blobClient.CreateSnapshot(meta);
                Console.WriteLine(res.Value.Snapshot);
            }



        }


        static void GetBlobFileInfo()
        {
            var blobClient = new Azure.Storage.Blobs.BlobClient(connsctionString, "test1", "images/girls/images.jpg");

            if (blobClient.Exists())
            {
                var res = blobClient.GetProperties();
                Console.WriteLine("ETag:" + res.Value.ETag);
            }
            else
            {
                Console.WriteLine("File Not Found");
            }
        }
        static void DeleteByETag()
        {

            var blobClient = new Azure.Storage.Blobs.BlobClient(connsctionString, "test1", "images/girls/images.jpg");
            Azure.ETag eTag = new Azure.ETag("0x8D8ABA4249FA91B");

            var blobUploadOptions = new BlobUploadOptions()
            {
                Conditions = new BlobRequestConditions()
                {
                    IfMatch = eTag,
                }
            };

            var res = blobClient.Upload(AppDomain.CurrentDomain.BaseDirectory + "sample2.jpg", blobUploadOptions);

            Console.WriteLine(res.Value);

        }
        static void CreateContainer()
        {
            var blobClient = new Azure.Storage.Blobs.BlobContainerClient(connsctionString, "test1");
            var res = blobClient.CreateIfNotExists();

            //如果 res 是  null 代表沒有建立成功，最有可能是已經建立過了，如果沒有其他 Exception 的話 
            if (res != null)
            {
                Console.WriteLine("SUCCESS");
            }
            else
            {
                Console.WriteLine("FAIL");
            }
        }

        static void DeleteDirAndSonFiles(string prefix)
        {
            var blobClient = new Azure.Storage.Blobs.BlobContainerClient(connsctionString, "test1");

            foreach (var blobItem in blobClient.GetBlobs(Azure.Storage.Blobs.Models.BlobTraits.None, Azure.Storage.Blobs.Models.BlobStates.None, prefix))
            {
                Console.WriteLine("\t" + blobItem.Name);
                var tmpClient = new Azure.Storage.Blobs.BlobClient(connsctionString, "test1", blobItem.Name);
                tmpClient.DeleteIfExists();
            }
        }
        static void ListDirs()
        {
            var blobClient = new Azure.Storage.Blobs.BlobContainerClient(connsctionString, "test1");

            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine("/[DIRS]");

            Console.ForegroundColor = ConsoleColor.Yellow;
            foreach (var blobItem in blobClient.GetBlobsByHierarchy(Azure.Storage.Blobs.Models.BlobTraits.None, Azure.Storage.Blobs.Models.BlobStates.None, "/"))
            {
                if (blobItem.IsPrefix)
                {

                    Console.WriteLine("\t" + blobItem.Prefix);
                }
            }

            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine("/images/[DIRS]");

            Console.ForegroundColor = ConsoleColor.Yellow;
            foreach (var blobItem in blobClient.GetBlobsByHierarchy(Azure.Storage.Blobs.Models.BlobTraits.None, Azure.Storage.Blobs.Models.BlobStates.None, "/", "images/"))
            {
                if (blobItem.IsPrefix)
                {

                    Console.WriteLine("\t" + blobItem.Prefix);
                }
            }

            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine("/images/girls/[DIRS]");

            Console.ForegroundColor = ConsoleColor.Yellow;
            foreach (var blobItem in blobClient.GetBlobsByHierarchy(Azure.Storage.Blobs.Models.BlobTraits.None, Azure.Storage.Blobs.Models.BlobStates.None, "/", "images/girls/"))
            {
                if (blobItem.IsPrefix)
                {

                    Console.WriteLine("\t" + blobItem.Prefix);
                }
            }

            Console.ForegroundColor = ConsoleColor.White;
        }
        static void ListFiles()
        {
            var blobClient = new Azure.Storage.Blobs.BlobContainerClient(connsctionString, "test1");

            foreach (var blobItem in blobClient.GetBlobs())
            //foreach (var  blobItem in blobClient.GetBlobs(Azure.Storage.Blobs.Models.BlobTraits.None,Azure.Storage.Blobs.Models.BlobStates.None,"images/"))
            {
                Console.WriteLine("\t" + blobItem.Name);
            }
        }

        static void DeleteFiles()
        {
            var blobClient = new Azure.Storage.Blobs.BlobClient(connsctionString, "test1", "images/girls/2020/samplex.jpg");
            blobClient.DeleteIfExists();
            Console.WriteLine("Success !!");
        }
        static void AddDirAndUploadFile()
        {


            //images/girls/2020/samplex.jpg 是我指定的儲存位置 就我們的看法是 samplex.jpg 存在 images資料夾下的girls資料夾下的 2020資料夾
            var blobClient = new Azure.Storage.Blobs.BlobClient(connsctionString, "test1", "images/girls/2020/samplex.jpg");

            //上傳 local 端檔案上去
            //true 代表複寫原本上面的
            blobClient.Upload(AppDomain.CurrentDomain.BaseDirectory + "sample1.jpg", true);

        }

    }
}
