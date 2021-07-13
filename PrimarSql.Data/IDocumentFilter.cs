using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data
{
    public interface IDocumentFilter
    {
        void Filter(string table, Dictionary<string, AttributeValue> document);
    }
}
