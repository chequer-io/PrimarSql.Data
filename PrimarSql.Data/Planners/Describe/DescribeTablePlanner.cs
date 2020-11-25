using System;
using System.Data.Common;
using System.Linq;
using Amazon.DynamoDBv2;
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
            var provider = new ListDataProvider();
            var tableDescription = Context.GetTableDescription(QueryInfo.TableName);

            var hashKeyName = tableDescription.KeySchema.First(schema => schema.KeyType == KeyType.HASH).AttributeName;
            var sortKeyName = tableDescription.KeySchema.FirstOrDefault(schema => schema.KeyType == KeyType.RANGE)?.AttributeName;

            provider.AddColumn("TableArn", typeof(string));
            provider.AddColumn("TableId", typeof(string));
            provider.AddColumn("TableName", typeof(string));
            provider.AddColumn("TableSizeBytes", typeof(long));
            provider.AddColumn("TableStatus", typeof(string));
            provider.AddColumn("LocalIndexCount", typeof(long));
            provider.AddColumn("GlobalIndexCount", typeof(long));
            provider.AddColumn("ItemCount", typeof(long));
            provider.AddColumn("CreateionDateTime", typeof(DateTime));
            provider.AddColumn("HashKeyName", typeof(string));
            provider.AddColumn("SortKeyName", typeof(string));
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
                sortKeyName,
                tableDescription.BillingModeSummary?.BillingMode.Value,
                tableDescription.BillingModeSummary?.LastUpdateToPayPerRequestDateTime
            );
            
            return new PrimarSqlDataReader(provider);
        }
    }
}
