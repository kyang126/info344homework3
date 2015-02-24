﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Microsoft.WindowsAzure.Storage.Table;


namespace ClassLibrary1
{
    public class cTable : TableEntity
    {

        public cTable(string value, string url, string title, string date, string error, string lastten, string rowkey, int tableSize)
        {
            this.PartitionKey = value;
            this.RowKey = rowkey;
            this.url = url;
            this.title = title;
            this.date = date;
            this.lastten = lastten;
            this.error = error;
            this.tableSize = tableSize;
        }

        public cTable() { }

        public int tableSize { get; set; }

        public string url { get; set; }
        public string error { get; set; }

        public string title { get; set; }

        public string date { get; set; }

        public string lastten { get; set; }



    }
}
