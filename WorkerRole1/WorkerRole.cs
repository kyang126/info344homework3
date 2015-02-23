using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using System.Configuration;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using System.IO;
using System.Text.RegularExpressions;
using ClassLibrary1;
using System.Text;
using HtmlAgilityPack;

namespace WorkerRole1
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        public override void Run()
        {
            Trace.TraceInformation("WorkerRole1 is running");
            try {
                getReference g = new getReference();
                CloudQueue cmd = g.commandQueue();
                cmd.CreateIfNotExists();
                while (true)
                {
                    CloudQueueMessage retrievedMessage = cmd.GetMessage();
                    if (retrievedMessage != null && retrievedMessage.AsString.Equals("start"))
                    {
                        cmd.DeleteMessage(retrievedMessage);
                        crawlRobots();
                    }
                    else if (retrievedMessage != null && retrievedMessage.AsString.Equals("stop"))
                    {
                        cmd.DeleteMessage(retrievedMessage);
                        break;
                    }
                }
                Thread.Sleep(50);
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public void crawlRobots()
        {
            List<String> noRobots = new List<String>();
            HashSet<String> allHtml = new HashSet<String>();
            var wc = new WebClient();
            String robot = "http://bleacherreport.com/robots.txt";
            int count = 0;
            List<String> yesRobots = new List<String>();
            getReference g = new getReference();
            CloudQueue queue = g.getQueue();
            for (int i = 0; i < 2; i++)
            {
                using (var sourceStream = wc.OpenRead(robot))
                {
                    using (var reader = new StreamReader(sourceStream))
                    {
                        while (reader.EndOfStream == false)
                        {

                            string line = reader.ReadLine();
                            if (!line.Contains("User-Agent"))
                            {

                                if (line.Contains("Sitemap:"))
                                {
                                    String newUrl = line.Replace("Sitemap:", "").Trim();

                                    if (robot.Contains("bleacher") && newUrl.Contains("nba"))
                                    {
                                        yesRobots.Add(newUrl);
                                    }
                                    else if (robot.Contains("cnn"))
                                    {
                                        yesRobots.Add(newUrl);
                                    }
                                }
                                if (line.Contains("Disallow:"))
                                {
                                    String newUrl = line.Replace("Disallow:", "").Trim();

                                    if (robot.Contains("bleacher"))
                                    {
                                        newUrl = "http://bleacherreport.com" + newUrl + "/";
                                    }
                                    else
                                    {
                                        newUrl = "http://cnn.com" + newUrl + "/";
                                    }

                                    noRobots.Add(newUrl);
                                }
                            }
                        }
                    }
                }
                count++;
                robot = "http://www.cnn.com/robots.txt";
            }
            allHtml = getAllHtml(yesRobots);
            crawlerUrls(allHtml, noRobots);
        }


        private HashSet<String> getAllHtml(List<String> o)
        {
            List<String> oldList = o;
            HashSet<String> htmlList = new HashSet<String>();
            int count = 0;
            getReference g = new getReference();
            CloudQueue queue = g.getQueue();
            while (count < oldList.Count)
            {
                WebClient web = new WebClient();
                String html = web.DownloadString(oldList.ElementAt(count));
                MatchCollection m1 = Regex.Matches(html, @"<loc>\s*(.+?)\s*</loc>", RegexOptions.Singleline);
                String index = oldList.ElementAt(count);
                foreach (Match m in m1)
                {
                    String url = m.Groups[1].Value;
                    if (url.Contains("xml") && ((url.Contains("2015") || !url.Contains("-20"))))
                    {
                        oldList.Add(url);
                    }
                    if (!url.Contains("xml"))
                    {
                        CloudQueueMessage message = new CloudQueueMessage(url);
                        queue.AddMessageAsync(message);
                        htmlList.Add(url);
                    }
                }
                count++;
            }
            return htmlList;
        }

        public void crawlerUrls(HashSet<String> duplicateList, List<String> noRobots)
        {
            HashSet<String> urlList = duplicateList;
            List<String> lastten = new List<String>();
            getReference g = new getReference();
            CloudQueue queue = g.getQueue();
            CloudTable table = g.getTable();
            CloudQueue cmd = g.commandQueue();
            queue.FetchAttributes();
            var limitCount = queue.ApproximateMessageCount.Value;
            int tableSize = 0;
            while (0 < limitCount)
            {
                CloudQueueMessage retrievedMessage = queue.GetMessage();
                try
                {
                    if (retrievedMessage != null)
                    {
                        HtmlWeb web = new HtmlWeb();
                        HtmlDocument document = web.Load(retrievedMessage.AsString);
                        String title = "";
                        HtmlNode node = document.DocumentNode.SelectSingleNode("//title");
                        if (node != null)
                        {
                            HtmlAttribute desc;
                            desc = node.Attributes["content"];
                            title = node.InnerHtml;
                        }

                        HtmlNode dateNode = document.DocumentNode.SelectSingleNode("//meta[(@itemprop='dateCreated')]");
                        String date = "";
                        if (dateNode != null)
                        {
                            date = dateNode.GetAttributeValue("content", "");
                        }

                        String tenUrls = "";
                        lastten.Add(retrievedMessage.AsString);
                        if (lastten.Count == 11)
                        {
                            lastten.RemoveAt(0);
                            tenUrls = String.Join(",", lastten);
                        }

                        queue.DeleteMessage(retrievedMessage);

                        String encodeUrl = EncodeUrlInKey(retrievedMessage.AsString);

                        tableSize++;

                        crawledTable ct = new crawledTable("index", retrievedMessage.AsString, null, date, null, null, encodeUrl, 0);
                        crawledTable dashboard = new crawledTable("dash", retrievedMessage.AsString, title, date, "DASHBOARD", tenUrls, "rowkey", tableSize);
                        TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(ct);
                        TableOperation insertOrReplaceOperation1 = TableOperation.InsertOrReplace(dashboard);
                        table.Execute(insertOrReplaceOperation);
                        table.Execute(insertOrReplaceOperation1);

                        String root = "";

                        if (retrievedMessage.AsString.Contains("bleacher"))
                        {
                            root = "bleacherreport.com";
                        }
                        else if (retrievedMessage.AsString.Contains("cnn"))
                        {
                            root = "cnn.com";
                        }

                        var rows = document.DocumentNode.SelectNodes("//a[@href]");
                        if (rows != null && rows.Count > 0)
                        {
                            foreach (var link in rows)
                            {
                                String url = link.Attributes["href"].Value;
                                if (url.StartsWith("//"))
                                {
                                    url = "http:" + url;
                                }
                                else if (url.StartsWith("/"))
                                {
                                    url = "http://" + root + url;
                                }
                                if (!urlList.Contains(url) && !noRobots.Contains(url) && (url.Contains(root + "/")))
                                {
                                    urlList.Add(url);
                                    CloudQueueMessage message = new CloudQueueMessage(url);
                                    queue.AddMessageAsync(message);
                                }
                            }
                        }
                        queue.FetchAttributes();
                        limitCount = queue.ApproximateMessageCount.Value;
                    }
                }
                catch (WebException e)
                {
                    queue.DeleteMessage(retrievedMessage);
                    crawledTable ct = new crawledTable("error", retrievedMessage.AsString, "No Title", "No date", e.Status.ToString(), null, "error url", 0);
                    TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(ct);
                    table.Execute(insertOrReplaceOperation);
                }
            }

        }

        private static String EncodeUrlInKey(String url)
        {
            var keyBytes = System.Text.Encoding.UTF8.GetBytes(url);
            var base64 = System.Convert.ToBase64String(keyBytes);
            return base64.Replace('/', '_');
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Trace.TraceInformation("WorkerRole1 has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("WorkerRole1 is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("WorkerRole1 has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");
                await Task.Delay(1000);
            }
        }
    }
}