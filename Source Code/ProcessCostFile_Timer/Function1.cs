using System;
using System.Data;
using System.Globalization;
using System.IO;
using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;

namespace ProcessCostFile_Timer
{



    public class ProcessCostFile
    {
        private readonly IConfiguration _configuration;

        public ProcessCostFile(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [FunctionName("ProcessCostFile")]
        public void Run([TimerTrigger("0 * */6 * * *")]TimerInfo myTimer, ILogger log)
        {
            string accessToken = _configuration["StorageKey"];
            string storageName = _configuration["StorageName"];

            string dbservername = _configuration["SqlServerName"];
            string dbname = _configuration["SqlDBName"];
            string dbusername = _configuration["SqlUserName"];
            string dbpassword = _configuration["SqlUserPassword"];
            string dbTableName = _configuration["SqlTableName"];

            string connectionString = @"Server =tcp:" + dbservername + ",1433;Initial Catalog=" + dbname + ";Persist Security Info=False;User ID=" + dbusername + ";Password=" + dbpassword + ";";

            int CurrMaxDateNum = DateTime.DaysInMonth(DateTime.Now.Year, DateTime.Now.Month);
            int CurrYear = DateTime.Now.Year;
            string CurrMonth = ("00" + (DateTime.Now.Month).ToString());
            CurrMonth = CurrMonth.Substring(CurrMonth.Length - 2, 2);
            string IsFullLoad = _configuration["IsFullLoad"];
            string FileSystemName = _configuration["FileSystemName"];
            string ParentDirectory = _configuration["ParentDirectoryName"];
            string ChildrenDirectories = _configuration["ChildrenDirectoryNames"];
            string[] Directories = ChildrenDirectories.Split(',');

            string StartDate = CurrYear.ToString() + CurrMonth + "01";
            string EndDate = CurrYear.ToString() + CurrMonth + CurrMaxDateNum.ToString();
            string DatePeriod = StartDate + "-" + EndDate;

            string currDate = (DateTime.Now).AddDays(-2).ToString("yyyy/MM/dd");
            currDate = currDate.Replace("-", "/");

            string DirectoryName = "";

            foreach (string dir in Directories)
            {
                DirectoryName = ParentDirectory + dir + "/" + DatePeriod;

                var client = GetDataLakeServiceClient(storageName, accessToken);
                DataLakeFileSystemClient filesystem = client.GetFileSystemClient(FileSystemName);

                ProcessLogData(filesystem, DirectoryName, IsFullLoad, currDate, connectionString, dbTableName);
            }

            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
        }

        private static DataLakeServiceClient GetDataLakeServiceClient(string accountName, string accountKey)
        {
            StorageSharedKeyCredential sharedKeyCredential =
                new StorageSharedKeyCredential(accountName, accountKey);

            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient(new Uri(dfsUri), sharedKeyCredential);
            return dataLakeServiceClient;
        }

        static void ProcessLogData(DataLakeFileSystemClient filesystem, string DirectoryName, string IsFullLoad, string currDate, string connectionString, string dbTableName)
        {

            string filename = "";
            DateTime filemodified;
            DateTime filemodifiedold = Convert.ToDateTime("1900/01/01");

            //try
            //{
                DataLakeDirectoryClient directoryClient = filesystem.GetDirectoryClient(DirectoryName);

                foreach (PathItem pathItem in directoryClient.GetPaths(false, false))
                {
                    filemodified = pathItem.LastModified.DateTime;
                    if (filemodified > filemodifiedold)
                    {
                        filename = pathItem.Name;
                        filemodifiedold = filemodified;
                    }
                }
                DirectoryName = DirectoryName + "/";
                DataLakeFileClient fileClient = directoryClient.GetFileClient(filename.Replace(DirectoryName, ""));

                Stream reader = fileClient.OpenRead();

                DataTable dt = new DataTable();

                StreamReader reader1 = new StreamReader(reader);

                string[] headers = reader1.ReadLine().Split(',');

                foreach (string header in headers)
                {
                    dt.Columns.Add(header);
                }

                while (!reader1.EndOfStream)
                {
                    string[] rows = Regex.Split(reader1.ReadLine(), ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }

                string[] selectedColumns = new[] { "date", "serviceFamily", "consumedService", "meterCategory", "meterSubCategory", "subscriptionName", "resourceGroupName", "location", "effectivePrice", "quantity", "billingCurrency" };

                // Create the DataView of the DataTable
                DataView view = new DataView(dt);
                // Create a new DataTable from the DataView with just the columns desired - and in the order desired
                DataTable selected = new DataView(dt).ToTable(false, selectedColumns);

                DataColumn dc = new DataColumn();
                dc.ColumnName = "newdate";
                dc.DataType = typeof(DateTime);

                selected.Columns.Add(dc);

                CultureInfo cultures = new CultureInfo("en-US");

                //update the new date column value
                foreach (DataRow row in selected.Rows)
                {
                    filename = row["date"].ToString().Replace(" ", "");
                    filename = filename.Substring(6, 4) + "/" + filename.Substring(0, 2) + "/" + filename.Substring(3, 2);
                    row["newdate"] = Convert.ToDateTime(filename, cultures);
                }

                // Create a DataView
                DataView dv = new DataView(selected);

                // Filter by an expression. Filter all rows where column 'Col' have values greater or equal than 3
                if (IsFullLoad != "1")
                {
                    dv.RowFilter = "newdate >= #" + currDate + "#";
                }

                selectedColumns = new[] { "newdate", "serviceFamily", "consumedService", "meterCategory", "meterSubCategory", "subscriptionName", "resourceGroupName", "location", "effectivePrice", "quantity", "billingCurrency" };

                DataTable dtFinal = new DataTable();
                dtFinal = dv.ToTable(false, selectedColumns);

                //string connectionString = @"Server =tcp:srv-dnaplatform-sqldb.database.windows.net,1433;Initial Catalog=db-dnaplatform-config;Persist Security Info=False;User ID=dnaadmin;Password=P@ssw0rd;";
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    if (IsFullLoad != "1")
                    {
                        string query = @"DELETE FROM ServiceUsage WHERE newdate >= '" + currDate + "'";

                        //define the SqlCommand object
                        SqlCommand cmd = new SqlCommand(query, connection);

                        //execute the SQLCommand
                        cmd.ExecuteNonQuery();
                    }

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                    {
                        foreach (DataColumn c in dtFinal.Columns)
                            bulkCopy.ColumnMappings.Add(c.ColumnName, c.ColumnName);

                        bulkCopy.DestinationTableName = dbTableName;

                        try
                        {
                            bulkCopy.WriteToServer(dtFinal);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                }
            //}
            //catch
            //{

            //}
        }
    }
}
