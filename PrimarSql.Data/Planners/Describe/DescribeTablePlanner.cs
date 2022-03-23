using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners.Describe
{
    internal sealed class DescribeTablePlanner : QueryPlanner<DescribeTableQueryInfo>
    {
        public DescribeTablePlanner(string tableName) : base(new DescribeTableQueryInfo(tableName))
        {
        }

        public override DbDataReader Execute()
        {
            return ExecuteAsync().Result;
        }

        public override async Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            var provider = new ListDataProvider();
            var tableDescription = await Context.GetTableDescriptionAsync(QueryInfo.TableName, cancellationToken);
            List<KeySchemaElement> keySchema = tableDescription.KeySchema;
            List<AttributeDefinition> attributeDefinitions = tableDescription.AttributeDefinitions;

            var hashKeyName = keySchema
                .First(schema => schema.KeyType == KeyType.HASH)
                .AttributeName;

            var sortKeyName = keySchema
                .FirstOrDefault(schema => schema.KeyType == KeyType.RANGE)?
                .AttributeName;

            var hashKeyType = attributeDefinitions
                .First(definition => definition.AttributeName == hashKeyName)
                .AttributeType
                .Value;

            var sortKeyType = attributeDefinitions
                .FirstOrDefault(definition => definition.AttributeName == sortKeyName)?
                .AttributeType
                .Value;

            provider.AddColumn("TableArn", typeof(string));
            provider.AddColumn("TableId", typeof(string));
            provider.AddColumn("TableName", typeof(string));
            provider.AddColumn("TableSizeBytes", typeof(long));
            provider.AddColumn("TableStatus", typeof(string));
            provider.AddColumn("LocalIndexCount", typeof(long));
            provider.AddColumn("GlobalIndexCount", typeof(long));
            provider.AddColumn("ItemCount", typeof(long));
            provider.AddColumn("CreationDateTime", typeof(DateTime));
            provider.AddColumn("HashKeyName", typeof(string));
            provider.AddColumn("HashKeyType", typeof(string));
            provider.AddColumn("SortKeyName", typeof(string));
            provider.AddColumn("SortKeyType", typeof(string));
            provider.AddColumn("BillingMode", typeof(string));
            provider.AddColumn("LastUpdateToPayPerRequestDateTime", typeof(DateTime));

            provider.AddRow(
                tableDescription.TableArn,
                tableDescription.TableId,
                tableDescription.TableName,
                tableDescription.TableSizeBytes,
                tableDescription.TableStatus.Value,
                tableDescription.LocalSecondaryIndexes.Count,
                tableDescription.GlobalSecondaryIndexes.Count,
                tableDescription.ItemCount,
                tableDescription.CreationDateTime,
                hashKeyName,
                hashKeyType,
                sortKeyName,
                sortKeyType,
                tableDescription.BillingModeSummary?.BillingMode.Value,
                tableDescription.BillingModeSummary?.LastUpdateToPayPerRequestDateTime
            );

            return new PrimarSqlDataReader(provider);
        }
    }
}
