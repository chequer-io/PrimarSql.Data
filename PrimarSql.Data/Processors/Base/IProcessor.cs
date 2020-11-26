using System.Collections.Generic;
using System.Data;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models.Columns;

namespace PrimarSql.Data.Processors
{
    internal interface IProcessor
    {
        Dictionary<string, AttributeValue> Current { get; set; }
        
        IColumn[] Columns { get; }
        
        DataTable GetSchemaTable();

        DataRow GetDataRow(string name);

        DataRow GetDataRow(int ordinal);
        
        object[] Process();

        Dictionary<string, AttributeValue> Filter();
    }
}
