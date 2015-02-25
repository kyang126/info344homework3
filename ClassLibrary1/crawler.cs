using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using HtmlAgilityPack;
using System.Diagnostics;

namespace ClassLibrary1
{
    public class crawler
    {
        HashSet<String> disallowedList;
        private PerformanceCounter theMemCounter = new PerformanceCounter("Memory", "Available MBytes");
        private PerformanceCounter cpuload = new PerformanceCounter("Processor", "% Processor Time", "_Total");

        public crawler()
        {
            disallowedList = new HashSet<string>();

        }

        //get xml method

        public void crawlRobots()
        {
            List<String> noRobots = new List<String>();
            HashSet<String> allHtml = new HashSet<String>();
            var wc = new WebClient();
            List<String> yesRobots = new List<String>();
            getReference g = new getReference();
            CloudQueue queue = g.getQueue();
            string[] robotSites = new string[2];
            robotSites[0] = "http://bleacherreport.com/robots.txt";
            robotSites[1] = "http://www.cnn.com/robots.txt";
            for (int i = 0; i < robotSites.Length; i++)
            {
                String robot = robotSites[i];
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
                                        newUrl = "http://bleacherreport.com" + newUrl;
                                    }
                                    else
                                    {
                                        newUrl = "http://cnn.com" + newUrl;
                                    }
                                    noRobots.Add(newUrl);
                                }
                            }
                        }
                    }
                }
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
            CloudTable table = g.getTable();
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
                        crawledTable dashboard = new crawledTable("dash", null, null, null, null, null, "rowkey", 0, null, null, "loading");
                        TableOperation insertOrReplaceOperation1 = TableOperation.InsertOrReplace(dashboard);
                        table.Execute(insertOrReplaceOperation1);
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
            List<String> lastten = new List<String>();
            List<String> errorList = new List<String>();
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

                        float memory = this.theMemCounter.NextValue();
                        float cpuUtilization = this.cpuload.NextValue();

                        tableSize++;

                        String errors = String.Join(",", errorList);

                        crawledTable ct = new crawledTable("index", retrievedMessage.AsString, title, date, null, null, encodeUrl, 0, null, null, null);
                        crawledTable dashboard = new crawledTable("dash", retrievedMessage.AsString, title, date, errors, tenUrls, "rowkey", tableSize, memory.ToString(), cpuUtilization + "%", "crawling");
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
                                Boolean isAllowed = true;
                                for (int i = 0; i < noRobots.Count; i++)
                                {
                                    String disallowedUrl = noRobots[i];
                                    if (url.Contains(disallowedUrl))
                                    {
                                        isAllowed = false;
                                        break;
                                    }
                                }

                                if (!duplicateList.Contains(url) && isAllowed && (url.Contains(root + "/")))
                                {
                                    duplicateList.Add(url);
                                    CloudQueueMessage message = new CloudQueueMessage(url);
                                    queue.AddMessageAsync(message);
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    errorList.Add(e.Message);
                }
                queue.FetchAttributes();
                limitCount = queue.ApproximateMessageCount.Value;
            }

        }

        private static String EncodeUrlInKey(String url)
        {
            var keyBytes = System.Text.Encoding.UTF8.GetBytes(url);
            var base64 = System.Convert.ToBase64String(keyBytes);
            return base64.Replace('/', '_');
        }



    }
}
