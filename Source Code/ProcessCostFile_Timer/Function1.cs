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

            int CurrMaxDateNum = 0;
            int CurrYear = 0;
            string CurrMonth = "";
            int CurrDay = DateTime.Now.Day;

            // Load the previous month data
	    // In case we need to load only previous month data
            if (_configuration["IsPrevMonthDataLoad"] == "1")
            {
                // Consider today is 1st day of the month
		CurrDay = 1;
            }

            // Get the previous month folder if current date is 1st
            if (CurrDay == 1)
            {
                // Get the previous month folder if current month is January
                // the month would be December and previous year
                if (DateTime.Now.Month == 1)
                {
                    CurrYear = DateTime.Now.Year - 1;
                    CurrMonth = "12";
                    CurrMaxDateNum = 31;
                }
                else
                {
                    // Get the previous month folder of current year
                    CurrYear = DateTime.Now.Year;
                    CurrMonth = ("00" + ((DateTime.Now.AddMonths(-1)).Month).ToString());
                    CurrMonth = CurrMonth.Substring(CurrMonth.Length - 2, 2);
                    CurrMaxDateNum = DateTime.DaysInMonth(DateTime.Now.Year, (DateTime.Now.AddMonths(-1)).Month);
                }
            }
            else
            {
                // Get the current month folder of current year
                CurrYear = DateTime.Now.Year;
                CurrMonth = ("00" + (DateTime.Now.Month).ToString());
                CurrMonth = CurrMonth.Substring(CurrMonth.Length - 2, 2);
                CurrMaxDateNum = DateTime.DaysInMonth(DateTime.Now.Year, DateTime.Now.Month);
            }

            string IsFullLoad = _configuration["IsFullLoad"];
            string FileSystemName = _configuration["FileSystemName"];
            string ParentDirectory = _configuration["ParentDirectoryName"];
            string ChildrenDirectories = _configuration["ChildrenDirectoryNames"];
            string[] Directories = ChildrenDirectories.Split(',');
            string StartDate = CurrYear.ToString() + CurrMonth + "01";
            string EndDate = CurrYear.ToString() + CurrMonth + CurrMaxDateNum.ToString();
            string DatePeriod = StartDate + "-" + EndDate;
            string DirectoryName = "";

            // process data for last 2 days to reduce the data process
            string currDate = (DateTime.Now).AddDays(-2).ToString("yyyy/MM/dd");

            // Do full load if the date difference is previous month
            if ((Convert.ToDateTime(currDate)).Month.ToString() != DateTime.Now.Month.ToString())
            {
                IsFullLoad = "1";
            }

            // Load the previous month data
            if (_configuration["IsPrevMonthDataLoad"] == "1")
            {
                currDate = CurrYear + "/" + CurrMonth + "/" + (CurrMaxDateNum).ToString();
            }

            currDate = currDate.Replace("-", "/");

            foreach (string dir in Directories)
            {
                DirectoryName = ParentDirectory + dir + "/" + DatePeriod;

                // Get the storage client
		var client = GetDataLakeServiceClient(storageName, accessToken);
                DataLakeFileSystemClient filesystem = client.GetFileSystemClient(FileSystemName);

		// Read the .csv file and load data to the sql database
                ProcessLogData(filesystem, DirectoryName, IsFullLoad, currDate, connectionString, dbTableName);
                log.LogInformation("filesystem - " + filesystem + " ; DirectoryName - " + DirectoryName + " ; CurrDate - " + currDate);
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

	    // Get the directory of the storage
            DataLakeDirectoryClient directoryClient = filesystem.GetDirectoryClient(DirectoryName);

            // Read only the latest file available in the folder
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

            StreamReader streader = new StreamReader(reader);

	    // Get the column headers
            string[] headers = streader.ReadLine().Split(',');

            foreach (string header in headers)
            {
                dt.Columns.Add(header);
            }

	    // Read the data and add to the data table
            while (!streader.EndOfStream)
            {
                string[] rows = Regex.Split(streader.ReadLine(), ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                DataRow dr = dt.NewRow();
                for (int i = 0; i < headers.Length; i++)
                {
                    dr[i] = rows[i];
                }
                dt.Rows.Add(dr);
            }

            // Filter the required columns to be stored in the database
            string[] selectedColumns = new[] { "date", "billingAccountName", "partnerName", "resellerName", "resellerMpnId", "customerTenantId", "customerName", "costCenter", "billingPeriodEndDate", "billingPeriodStartDate", "servicePeriodEndDate", "servicePeriodStartDate" , "serviceFamily", "productOrderId", "productOrderName", "consumedService", "meterId", "meterName", "meterCategory", "meterSubCategory", "meterRegion", "ProductId", "ProductName", "SubscriptionId", "subscriptionName", "publisherType", "publisherId", "publisherName", "resourceGroupName", "ResourceId", "resourceLocation", "location", "effectivePrice", "quantity", "unitOfMeasure", "chargeType", "billingCurrency", "pricingCurrency", "costInBillingCurrency", "costInUsd", "exchangeRatePricingToBilling", "exchangeRateDate", "serviceInfo1", "serviceInfo2", "additionalInfo", "tags", "PayGPrice", "frequency", "term", "reservationId", "reservationName", "pricingModel", "unitPrice" };

            // Create the DataView of the DataTable
            DataView view = new DataView(dt);
            
	    // Create a new DataTable from the DataView with just the columns desired - and in the order desired
            DataTable selected = new DataView(dt).ToTable(false, selectedColumns);

	    // Add a new date column as the csv file date is not in uniform format
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

	    // Select the final column list
            selectedColumns = new[] { "newdate", "billingAccountName", "partnerName", "resellerName", "resellerMpnId", "customerTenantId", "customerName", "costCenter", "billingPeriodEndDate", "billingPeriodStartDate", "servicePeriodEndDate", "servicePeriodStartDate" /*, "date"*/, "serviceFamily", "productOrderId", "productOrderName", "consumedService", "meterId", "meterName", "meterCategory", "meterSubCategory", "meterRegion", "ProductId", "ProductName", "SubscriptionId", "subscriptionName", "publisherType", "publisherId", "publisherName", "resourceGroupName", "ResourceId", "resourceLocation", "location", "effectivePrice", "quantity", "unitOfMeasure", "chargeType", "billingCurrency", "pricingCurrency", "costInBillingCurrency", "costInUsd", "exchangeRatePricingToBilling", "exchangeRateDate", "serviceInfo1", "serviceInfo2", "additionalInfo", "tags", "PayGPrice", "frequency", "term", "reservationId", "reservationName", "pricingModel", "unitPrice" };


            DataTable dtFinal = new DataTable();
            dtFinal = dv.ToTable(false, selectedColumns);

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                if (IsFullLoad != "1")
                {

                   // Get the subscription name and delete data from table
                    string subscriptionName = dtFinal.Rows[1]["subscriptionName"].ToString();
                    string query = @"DELETE FROM " + dbTableName + " WHERE subscriptionName = '" + subscriptionName + "' and newdate >= '" + currDate + "'";

                    //define the SqlCommand object
                    SqlCommand cmd = new SqlCommand(query, connection);

                    //execute the SQLCommand
                    cmd.ExecuteNonQuery();
                 }
		 
		 // Bulk copy of the csv file data to the sql table
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
        }
    }
}
