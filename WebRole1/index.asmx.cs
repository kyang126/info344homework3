using HtmlAgilityPack;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Web;
using System.Web.Services;
using ClassLibrary1;
using System.Text;

namespace WebRole1
{
    /// <summary>
    /// Summary description for index
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    [System.Web.Script.Services.ScriptService]
    public class index : System.Web.Services.WebService
    {
        private List<String> htmlFiles = new List<String>();


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
        public void stopCrawl()
        {
            getReference g = new getReference();
            CloudQueue queue = g.commandQueue();
            queue.CreateIfNotExists();
            CloudQueueMessage message = new CloudQueueMessage("stop");
            queue.AddMessage(message);

            CloudQueue storage = g.getQueue();
            storage.Clear();
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

        [WebMethod]
        public String getcmdCount()
        {
            getReference g = new getReference();
            CloudQueue queue = g.commandQueue();
            queue.CreateIfNotExists();
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
        public void crawlRobots()
        {
            List<String> noRobots = new List<String>();
            HashSet<String> allHtml = new HashSet<String>();
            var wc = new WebClient();
            String robot = "http://www.cnn.com/robots.txt";
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

        [WebMethod]
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
            int count = 0;
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
                        
                        cTable ct = new cTable("index", retrievedMessage.AsString, null, date, null, null, encodeUrl, 0);
                        cTable dashboard = new cTable("dash", retrievedMessage.AsString, title, date, "DASHBOARD", tenUrls, "rowkey", 0);
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

                        var rows= document.DocumentNode.SelectNodes("//a[@href]");
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
                    cTable ct = new cTable("error", retrievedMessage.AsString, "No Title", "No date", e.Status.ToString(), null, "error url", 0);
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

        private static String DecodeUrlInKey(String encodedKey)
        {
            var base64 = encodedKey.Replace('_', '/');
            byte[] bytes = System.Convert.FromBase64String(base64);
            return System.Text.Encoding.UTF8.GetString(bytes);
        }



    }


}
