using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
using ClassLibrary1;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;

namespace WebRole1
{
    /// <summary>
    /// Summary description for dashboard
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    [System.Web.Script.Services.ScriptService]
    public class dashboard : System.Web.Services.WebService
    {


        [WebMethod]
        public void startCrawl()
        {
            getReference g = new getReference();
            CloudQueue queue = g.commandQueue();
            queue.CreateIfNotExists();
            CloudQueueMessage message = new CloudQueueMessage("start");
            queue.AddMessage(message);
        }


        [WebMethod]
        public String getCmd()
        {
            getReference g = new getReference();
            CloudQueue queue = g.commandQueue();
            queue.Clear();
            queue.FetchAttributes();
            int approximateMessagesCount = queue.ApproximateMessageCount.Value;
            return "" + approximateMessagesCount;
        }

        [WebMethod]
        public void clearCmd()
        {
            getReference g = new getReference();
            CloudQueue queue = g.commandQueue();
            queue.Clear();
        }

        [WebMethod]
        public void stopCrawl()
        {
            getReference g = new getReference();
            CloudQueue queue = g.commandQueue();
            queue.CreateIfNotExists();
            CloudQueueMessage message = new CloudQueueMessage("stop");
            queue.AddMessage(message);
        }

        [WebMethod]
        public List<String> lastTen()
        {
            getReference g = new getReference();
            CloudTable table = g.getTable();
            List<String> t = new List<String>();
            TableOperation retrieveOperation = TableOperation.Retrieve<crawledTable>("dash", "rowkey");
            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);
            String value = ((crawledTable)retrievedResult.Result).lastten;
            string[] values = value.Split(',');
            for (int i = 0; i < values.Length; i++)
            {
                t.Add(values[i]);
            }
                return t;
        }

        [WebMethod]
        public int tableSize()
        {
            getReference g = new getReference();
            CloudTable table = g.getTable();
            TableOperation retrieveOperation = TableOperation.Retrieve<crawledTable>("dash", "rowkey");
            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);
            int value = ((crawledTable)retrievedResult.Result).tableSize;
            return value;

        }



        [WebMethod]
        public void clearQ()
        {
            getReference g = new getReference();
            CloudQueue queue = g.getQueue();

            queue.Clear();
        }

        [WebMethod]
        public void deleteTable()
        {
            getReference g = new getReference();
            CloudTable table = g.getTable();
            table.Delete();
        }

        [WebMethod]
        public String getQueueCount()
        {
            getReference g = new getReference();
            CloudQueue queue = g.getQueue();
            queue.CreateIfNotExists();
            queue.FetchAttributes();
            int approximateMessagesCount = queue.ApproximateMessageCount.Value;
            return "" + approximateMessagesCount;
        }
    }
}
