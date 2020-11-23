using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Planners.Table;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class CreateTablePlanner : QueryPlanner<CreateTableQueryInfo>
    {
        private const int DefaultCapacity = 10;

        private List<KeySchemaElement> GetKeySchema(string hashKey, string sortKey)
        {
            var keySchema = new List<KeySchemaElement>
            {
                new KeySchemaElement(hashKey, KeyType.HASH)
            };

            if (!string.IsNullOrWhiteSpace(sortKey))
                keySchema.Add(new KeySchemaElement(sortKey, KeyType.RANGE));

            return keySchema;
        }

        public override DbDataReader Execute()
        {
            try
            {
                QueryContext.GetTableDescription(QueryInfo.TableName);
                
                if (QueryInfo.SkipIfExists)
                    return new PrimarSqlDataReader(new EmptyDataProvider());
            }
            catch (Exception)
            {
                // ignored
            }
            
                
            QueryInfo.Validate();
            
            var request = new CreateTableRequest
            {
                TableName = QueryInfo.TableName,
                BillingMode = QueryInfo.TableBillingMode switch
                {
                    TableBillingMode.Provisoned => BillingMode.PROVISIONED,
                    TableBillingMode.PayPerRequest => BillingMode.PAY_PER_REQUEST,
                    _ => BillingMode.PROVISIONED
                },
                ProvisionedThroughput = new ProvisionedThroughput(QueryInfo.ReadCapacity ?? DefaultCapacity, QueryInfo.WriteCapacity ?? DefaultCapacity)
            };

            var hashKey = QueryInfo.TableColumns.First(column => column.IsHashKey).ColumnName;
            var sortKey = QueryInfo.TableColumns.FirstOrDefault(column => column.IsSortKey)?.ColumnName;

            request.KeySchema = GetKeySchema(hashKey, sortKey);

            foreach (var index in QueryInfo.IndexDefinitions)
            {
                if (index.IsLocalIndex)
                {
                    var localIndex = new LocalSecondaryIndex
                    {
                        IndexName = index.IndexName,
                        Projection = index.Projection,
                        KeySchema = index.KeySchema
                    };

                    request.LocalSecondaryIndexes.Add(localIndex);
                }
                else
                {
                    var globalIndex = new GlobalSecondaryIndex
                    {
                        IndexName = index.IndexName,
                        Projection = index.Projection,
                        KeySchema = index.KeySchema
                    };

                    request.GlobalSecondaryIndexes.Add(globalIndex);
                }
            }

            foreach (var column in QueryInfo.TableColumns)
            {
                request.AttributeDefinitions.Add(new AttributeDefinition(column.ColumnName, DataTypeToScalarAttributeType(column.DataType)));
            }

            QueryContext.Client.CreateTableAsync(request).Wait();
            
            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
