using System;
using System.Data.Common;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class DropIndexPlanner : QueryPlanner<DropIndexQueryInfo>
    {
        public override DbDataReader Execute()
        {
            try
            {
                QueryContext.Client.UpdateTableAsync(new UpdateTableRequest
                {
                    TableName = QueryInfo.TableName,
                    GlobalSecondaryIndexUpdates =
                    {
                        new GlobalSecondaryIndexUpdate
                        {
                            Delete =
                            {
                                IndexName = QueryInfo.TargetIndex
                            }
                        }
                    }
                }).Wait();
            }
            catch (AggregateException e)
            {
                var innerException = e.InnerExceptions[0];
                throw new Exception($"Error while Drop index '{QueryInfo.TableName}.{QueryInfo.TargetIndex}'{Environment.NewLine}{innerException.Message}");
            }

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
