using System;
using System.Collections.Generic;
using System.Data.Common;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Planners
{
    internal abstract class QueryPlanner
    {
        public QueryContext QueryContext { get; set; }
        
        public abstract DbDataReader Execute();
    }
}
