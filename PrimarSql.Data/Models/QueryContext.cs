using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
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

        public PrimarSqlCommand Command { get; set; }

        public QueryContext(AmazonDynamoDBClient client, PrimarSqlCommand command)
        {
            Command = command;
            SetClient(client);
        }

        public void SetClient(AmazonDynamoDBClient client)
        {
            _client = client;
        }

        public async Task<TableDescription> GetTableDescriptionAsync(string tableName, CancellationToken cancellationToken = default)
        {
            if (!_tableDescriptions.ContainsKey(tableName))
                _tableDescriptions[tableName] = (await _client.DescribeTableAsync(tableName, cancellationToken)).Table;

            return _tableDescriptions[tableName];
        }
    }
}
