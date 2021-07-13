using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data.Models
{
    internal sealed class QueryContext
    {
        private readonly Dictionary<string, TableDescription> _tableDescriptions = new Dictionary<string, TableDescription>();
        private AmazonDynamoDBClient _client;

        public AmazonDynamoDBClient Client => _client ?? throw new NullReferenceException("Client should not be null.");

        public IList<IDocumentFilter> DocumentFilters { get; set; }

        public QueryContext(AmazonDynamoDBClient client)
        {
            SetClient(client);
        }

        public void SetClient(AmazonDynamoDBClient client)
        {
            _client = client;
        }

        public TableDescription GetTableDescription(string tableName)
        {
            if (!_tableDescriptions.ContainsKey(tableName))
                _tableDescriptions[tableName] = _client.DescribeTableAsync(tableName).Result.Table;

            return _tableDescriptions[tableName];
        }
    }
}
