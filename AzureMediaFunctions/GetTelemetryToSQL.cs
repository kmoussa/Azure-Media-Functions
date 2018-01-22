
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
using Microsoft.IdentityModel.Protocols;
using System.Data.SqlClient;
using System.Text;

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
        private static string sqlconnectionstring = Environment.GetEnvironmentVariable("DBtelemetryEntities");
        static CloudStorageAccount storageAccount = null;
        static CloudTableClient tableClient = null;
      
        [FunctionName("GetTelemetryToSQL")]
        public static async System.Threading.Tasks.Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "")]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("Migration function has been triggered.");
            try
            {
                // Parse the connection string and return a reference to the storage account.
                storageAccount = CloudStorageAccount.Parse(storageconnectionstring);


              
                // Create the table client.

                tableClient = storageAccount.CreateCloudTableClient();
                StringBuilder sb = new StringBuilder();
                sb.AppendLine("select top 1 * from streamingdata order by timestamp desc");
               DataSet ds = CreateCommand(sb.ToString(), sqlconnectionstring, true);

                //check if the table has data 
                if (ds.Tables[0].Rows.Count > 0)
                {
                    //check for the latest table migration
                    string timestamp = ds.Tables[0].Rows[0][3].ToString();
                    DateTime newTS = DateTime.Parse(timestamp.Split(' ')[0]);
                    string compareTS = newTS.ToString("yyyyMMdd").Replace("/", "");
                    IEnumerable<CloudTable> tables = tableClient.ListTables("TelemetryMetrics").
                        Where(x => int.Parse(x.Name.Split('s')[1]) >= int.Parse(compareTS));

                    copyTelemtery(tables,DateTimeOffset.Parse(timestamp), "TelemetryMetrics" + compareTS, log);
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
        private static DataSet CreateCommand(string queryString,
    string connectionString,bool select)
        {
            using (SqlConnection connection = new SqlConnection(
                       connectionString))
            {
                connection.Open();
                SqlCommand command = new SqlCommand();
                command.Connection = connection;
                command.CommandText = queryString;
                DataSet ds = new DataSet();
                if (select)
                {
                    SqlDataAdapter sda = new SqlDataAdapter();
                    sda.SelectCommand = command;
                    sda.Fill(ds);
                    command.Dispose();
                    connection.Dispose();
                    return ds;
                }
                else
                {
                    command.ExecuteNonQuery();
                    command.Dispose();
                    connection.Dispose();
                    return null;
                }
                 
               
            }
            
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
                            StringBuilder sb = new StringBuilder();
                          
                            
                            foreach (var item in T)
                            {

                                sb.Append("insert into streamingdata (PartitionKey,RowKey,TimeStamp, ObservationTime, Type,Name,ServiceId,HostName,Statuscode,resultcode,requestCount, bytessent,serverlatency,e2elatency) values ('");
                                sb.Append(item.PartitionKey + "','");
                                sb.Append(item.RowKey + "',cast('");
                                sb.Append(item.Timestamp + "' as datetimeoffset),cast('");
                                
                                sb.Append(DateTime.Parse(item.ObservedTime.ToString()) + "' as datetime),'");
                                sb.Append(item.Type + "','");
                                sb.Append(item.Name + "','");
                                sb.Append(item.ServiceId + "','");
                                sb.Append(item.HostName + "','");
                                sb.Append(item.StatusCode.ToString() + "','");
                                sb.Append(item.ResultCode + "',");
                                sb.Append(item.RequestCount + ",");
                                sb.Append(item.BytesSent + ",");
                                sb.Append(item.ServerLatency + ",");
                                sb.Append(item.E2ELatency + ");");
                                 
                               
                                log.Info("inserting entity " + count++ + " from table " + table.Name);
                            }
                            DataSet ds = CreateCommand(sb.ToString(), sqlconnectionstring, false);
                            log.Info("Data Submitted successfully in the table ");
                        }
                    }
                    else
                    {
                        query = new TableQuery<TelemetryEntity>();
                        IEnumerable<TelemetryEntity> T = table.ExecuteQuery(query).OrderBy(x => x.Timestamp);
                        log.Info("Reading Table " + table.Name);
                        log.Info("Found " + T.Count() + " records inside");
                        int count = 0;

                        StringBuilder sb = new StringBuilder();
                        foreach (var item in T)
                        {
                            sb.Append("insert into streamingdata (PartitionKey,RowKey,TimeStamp, ObservationTime, Type,Name,ServiceId,HostName,Statuscode,resultcode,requestCount, bytessent,serverlatency,e2elatency) values ('");
                            sb.Append(item.PartitionKey + "','");
                            sb.Append(item.RowKey + "',cast('");
                            sb.Append(item.Timestamp + "' as datetimeoffset),cast('");

                            sb.Append(item.ObservedTime.ToString() + "' as datetime),'");
                            sb.Append(item.Type + "','");
                            sb.Append(item.Name + "','");
                            sb.Append(item.ServiceId + "','");
                            sb.Append(item.HostName + "','");
                            sb.Append(item.StatusCode.ToString() + "','");
                            sb.Append(item.ResultCode + "',");
                            sb.Append(item.RequestCount + ",");
                            sb.Append(item.BytesSent + ",");
                            sb.Append(item.ServerLatency + ",");
                            sb.Append(item.E2ELatency + ");");



                            log.Info("inserting entity " + count++ + " from table " + table.Name);
                            }
                            DataSet ds = CreateCommand(sb.ToString(), sqlconnectionstring, false);
                        log.Info("Data Submitted successfully in the table ");

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
                    StringBuilder sb = new StringBuilder();
                    foreach (var item in T)
                    {
                        sb.Append("insert into streamingdata (PartitionKey,RowKey,TimeStamp, ObservationTime, Type,Name,ServiceId,HostName,Statuscode,resultcode,requestCount, bytessent,serverlatency,e2elatency) values ('");
                        sb.Append(item.PartitionKey + "','");
                        sb.Append(item.RowKey + "',cast('");
                        sb.Append(item.Timestamp + "' as datetimeoffset),cast('");

                        sb.Append(item.ObservedTime.ToString() + "' as datetime),'");
                        sb.Append(item.Type + "','");
                        sb.Append(item.Name + "','");
                        sb.Append(item.ServiceId + "','");
                        sb.Append(item.HostName + "','");
                        sb.Append(item.StatusCode.ToString() + "','");
                        sb.Append(item.ResultCode + "',");
                        sb.Append(item.RequestCount + ",");
                        sb.Append(item.BytesSent + ",");
                        sb.Append(item.ServerLatency + ",");
                        sb.Append(item.E2ELatency + ");");



                        log.Info("inserting entity " + count++ + " from table " + table.Name);
                    }
                    DataSet ds = CreateCommand(sb.ToString(), sqlconnectionstring, false);
                    log.Info("Data Submitted successfully in the table ");
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
