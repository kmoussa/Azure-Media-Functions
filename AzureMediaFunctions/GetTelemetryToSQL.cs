using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Data;
using System.Data.Sql;
using shahidtelemetryModel;
using Microsoft.IdentityModel.Protocols;

namespace AzureMediaFunctions
{


    public static class GetTelemetryToSQL
    {
        private static string domain = Environment.GetEnvironmentVariable("domain");
        private static string restendpoint = Environment.GetEnvironmentVariable("restendpoint");
        private static string servicekey = Environment.GetEnvironmentVariable("servicekey");
        private static string serviceprincipaleId = Environment.GetEnvironmentVariable("serviceprincipleId");
        private static string storagekey = Environment.GetEnvironmentVariable("storagekey");
        private static string storagename = Environment.GetEnvironmentVariable("storagename");
        private static string storageconnectionstring = Environment.GetEnvironmentVariable("storageconnectionstring");
        private static string sqlconnectionstring = Environment.GetEnvironmentVariable("shahidtelemetryEntities");
        static CloudStorageAccount storageAccount = null;
        static CloudTableClient tableClient = null;
      
        [FunctionName("GetTelemetryToSQL")]
        public static async System.Threading.Tasks.Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "")]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");
            try
            {
                 string connString = ConfigurationManager.ConnectionStrings["shahidtelemetryEntities"].ConnectionString;

                 shahidtelemetryEntities db = new shahidtelemetryEntities(connString);
                // Fetching the name from the path parameter in the request URL

                // Parse the connection string and return a reference to the storage account.
                storageAccount = CloudStorageAccount.Parse(storageconnectionstring);

                // Create the table client.

                tableClient = storageAccount.CreateCloudTableClient();


                //check if the table has data 
                if (db.StreamingDatas.Count() > 0)
                {
                    //check for the latest table migration
                    StreamingData sd = db.StreamingDatas.OrderByDescending(x => x.TimeStamp).First();
                    DateTime newTS = DateTime.Parse(sd.TimeStamp.ToString().Split(' ')[0]);
                    string compareTS = newTS.ToString("yyyyMMdd").Replace("/", "");
                    IEnumerable<CloudTable> tables = tableClient.ListTables("TelemetryMetrics").
                        Where(x => int.Parse(x.Name.Split('s')[1]) >= int.Parse(compareTS));

                    copyTelemtery(tables, sd.TimeStamp, "TelemetryMetrics" + compareTS, log);
                }
                else
                {
                    //start Full system migration
                    IEnumerable<CloudTable> tables = tableClient.ListTables("TelemetryMetrics");
                    copyTelemtery(tables, null, "", log);
                }

                Console.ReadLine();

            }
            catch (Exception ex)
            {

                log.Info(ex.Message, null);
            }
            return req.CreateResponse(HttpStatusCode.OK);

        }
        public static void copyTelemtery(IEnumerable<CloudTable> tables, DateTimeOffset? lastdate, string tablename, TraceWriter log)
        {
            log.Info("Found " + tables.Count() + " tables");
            TableQuery<TelemetryEntity> query;
            if (lastdate != null)
            {


                int i = 0;
                foreach (var table in tables)
                {
                    if (i == 0)
                    {
                        if (table.Name == tablename)
                        {
                            DateTimeOffset newlastdate = (DateTimeOffset)lastdate;
                            //lastdate = lastdate.Value.ToUniversalTime().UtcDateTime;
                            query = new TableQuery<TelemetryEntity>().Where(TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThan, newlastdate));
                            IEnumerable<TelemetryEntity> T = table.ExecuteQuery(query).OrderBy(x => x.Timestamp);
                            log.Info("Reading Table " + table.Name);
                            log.Info("Found " +T.Count() + " records inside");
                            int count = 0;
                            foreach (var item in T)
                            {

                                StreamingData sd = new StreamingData();
                                sd.PartitionKey = item.PartitionKey;
                                sd.RowKey = item.RowKey;
                                sd.TimeStamp = item.Timestamp;

                                sd.ObservationTime = DateTime.Parse(item.ObservedTime.ToString());


                                sd.Type = item.Type;
                                sd.Name = item.Name;
                                sd.ServiceId = item.ServiceId;


                                sd.HostName = item.HostName;
                                sd.StatusCode = item.StatusCode.ToString();
                                sd.ResultCode = item.ResultCode;

                                sd.RequestCount = item.RequestCount;
                                sd.BytesSent = item.BytesSent;
                                sd.ServerLatency = item.ServerLatency;
                                sd.E2ELatency = item.E2ELatency;

                                db.StreamingDatas.Add(sd);
                                db.SaveChanges();
                                log.Info("inserting entity " + count++ + " from table " + table.Name);
                            }
                        }
                    }
                    else
                    {
                        query = new TableQuery<TelemetryEntity>();
                        IEnumerable<TelemetryEntity> T = table.ExecuteQuery(query).OrderBy(x => x.Timestamp);
                        log.Info("Reading Table " + table.Name);
                        log.Info("Found " + T.Count() + " records inside");
                        int count = 0;
                        foreach (var item in T)
                        {


                            StreamingData sd = new StreamingData();
                            sd.PartitionKey = item.PartitionKey;
                            sd.RowKey = item.RowKey;
                            sd.TimeStamp = item.Timestamp;
                            sd.ObservationTime = DateTime.Parse(item.ObservedTime.ToString());
                            sd.Type = item.Type;
                            sd.Name = item.Name;
                            sd.ServiceId = item.ServiceId;

                            sd.HostName = item.HostName;
                            sd.StatusCode = item.StatusCode.ToString();
                            sd.ResultCode = item.ResultCode;
                            sd.RequestCount = item.RequestCount;
                            sd.BytesSent = item.BytesSent;
                            sd.ServerLatency = item.ServerLatency;
                            sd.E2ELatency = item.E2ELatency;
                            db.StreamingDatas.Add(sd);
                            db.SaveChanges();
                            log.Info("inserting entity " + count++ + " from table " + table.Name);
                        }
                    }
                    i++;

                }
            }
            else
            {
                foreach (var table in tables)
                {
                    query = new TableQuery<TelemetryEntity>();
                    IEnumerable<TelemetryEntity> T = table.ExecuteQuery(query).OrderBy(x => x.Timestamp);
                    log.Info("Reading Table " + table.Name);
                    log.Info("Found " + T.Count() + " records inside");
                    int count = 0;
                    foreach (var item in T)
                    {


                        StreamingData sd = new StreamingData();
                        sd.PartitionKey = item.PartitionKey;
                        sd.RowKey = item.RowKey;
                        sd.TimeStamp = item.Timestamp;
                        sd.ObservationTime = DateTime.Parse(item.ObservedTime.ToString());
                        sd.Type = item.Type;
                        sd.Name = item.Name;
                        sd.ServiceId = item.ServiceId;

                        sd.HostName = item.HostName;
                        sd.StatusCode = item.StatusCode.ToString();
                        sd.ResultCode = item.ResultCode;
                        sd.RequestCount = item.RequestCount;
                        sd.BytesSent = item.BytesSent;
                        sd.ServerLatency = item.ServerLatency;
                        sd.E2ELatency = item.E2ELatency;
                        db.StreamingDatas.Add(sd);
                        db.SaveChanges();
                        log.Info("inserting entity " + count++ + " from table " + table.Name);
                    }
                }


            }


        }
    }
    public class TelemetryEntity : TableEntity
    {
        public TelemetryEntity(string partitionkey, string rowkey, DateTimeOffset timestamp, string type, string name, DateTimeOffset observedTime, Guid serviceid,
            string hostname, int statuscode, string resultcode, int requestcount, int bytessent, int serverlatency, int e2elatency)
        {
            this.PartitionKey = partitionkey;
            this.RowKey = rowkey;
            this.Timestamp = timestamp;
            this.Type = type;
            this.Name = name;
            this.ObservedTime = observedTime;
            this.ServiceId = serviceid;
            this.HostName = hostname;
            this.StatusCode = statuscode;
            this.ResultCode = resultcode;
            this.RequestCount = requestcount;
            this.BytesSent = bytessent;
            this.ServerLatency = serverlatency;

            this.E2ELatency = e2elatency;
        }

        public TelemetryEntity() { }



        public string Type { get; set; }

        public string Name { get; set; }

        public DateTimeOffset ObservedTime { get; set; }

        public Guid ServiceId { get; set; }

        public string HostName { get; set; }

        public int StatusCode { get; set; }

        public string ResultCode { get; set; }

        public int RequestCount { get; set; }

        public int BytesSent { get; set; }

        public int ServerLatency { get; set; }

        public int E2ELatency { get; set; }


    }
}
