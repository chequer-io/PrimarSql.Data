using System.Collections.Generic;
using System.Data;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json.Linq;

namespace PrimarSql.Data.Processors
{
    internal interface IProcessor
    {
        DataTable GetSchemaTable();

        DataRow GetDataRow(string name);

        DataRow GetDataRow(int ordinal);
        
        JToken[] Process(Dictionary<string, AttributeValue> row);
    }
}
