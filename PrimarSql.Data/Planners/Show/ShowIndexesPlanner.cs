using System.Data.Common;
using System.Linq;
using Amazon.DynamoDBv2;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners.Show
{
    internal sealed class ShowIndexesPlanner : QueryPlanner<ShowIndexesQueryInfo>
    {
        public ShowIndexesPlanner(string tableName) : base(new ShowIndexesQueryInfo(tableName))
        {
        }

        public override DbDataReader Execute()
        {
            var provider = new ListDataProvider();
            var tableDescription = Context.GetTableDescription(QueryInfo.TableName);

            provider.AddColumn("IndexType", typeof(string));
            provider.AddColumn("IndexArn", typeof(string));
            provider.AddColumn("IndexName", typeof(string));
            provider.AddColumn("IndexSizeBytes", typeof(long));
            provider.AddColumn("ItemCount", typeof(long));
            provider.AddColumn("HashKeyName", typeof(string));
            provider.AddColumn("SortKeyName", typeof(string));
            provider.AddColumn("ProjectionType", typeof(string));
            provider.AddColumn("NonKeyAttributes", typeof(string));

            foreach (var localIndex in tableDescription.LocalSecondaryIndexes)
            {
                var hashKeyName = localIndex.KeySchema.First(schema => schema.KeyType == KeyType.HASH).AttributeName;
                var sortKeyName = localIndex.KeySchema.FirstOrDefault(schema => schema.KeyType == KeyType.RANGE)?.AttributeName;

                provider.AddRow(
                    "LOCAL",
                    localIndex.IndexArn,
                    localIndex.IndexName,
                    localIndex.IndexSizeBytes,
                    localIndex.ItemCount,
                    hashKeyName,
                    sortKeyName,
                    localIndex.Projection.ProjectionType.Value,
                    string.Join(", ", localIndex.Projection.NonKeyAttributes)
                );
            }

            foreach (var globalIndex in tableDescription.GlobalSecondaryIndexes)
            {
                var hashKeyName = globalIndex.KeySchema.First(schema => schema.KeyType == KeyType.HASH).AttributeName;
                var sortKeyName = globalIndex.KeySchema.FirstOrDefault(schema => schema.KeyType == KeyType.RANGE)?.AttributeName;

                provider.AddRow(
                    "GLOBAL",
                    globalIndex.IndexArn,
                    globalIndex.IndexName,
                    globalIndex.IndexSizeBytes,
                    globalIndex.ItemCount,
                    hashKeyName,
                    sortKeyName,
                    globalIndex.Projection.ProjectionType.Value,
                    string.Join(", ", globalIndex.Projection.NonKeyAttributes)
                );
            }

            return new PrimarSqlDataReader(provider);
        }
    }
}
