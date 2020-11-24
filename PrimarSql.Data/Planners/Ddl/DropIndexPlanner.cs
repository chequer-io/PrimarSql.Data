using System;
using System.Collections.Generic;
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
                var request = new UpdateTableRequest
                {
                    TableName = QueryInfo.TableName,
                    GlobalSecondaryIndexUpdates = new List<GlobalSecondaryIndexUpdate>
                    {
                        new GlobalSecondaryIndexUpdate
                        {
                            Delete = new DeleteGlobalSecondaryIndexAction
                            {
                                IndexName = QueryInfo.TargetIndex
                            }
                        }
                    }
                };

                Context.Client.UpdateTableAsync(request).Wait();
            }
            catch (AggregateException e)
            {
                var innerException = e.InnerExceptions[0];
                throw new Exception($"Error while drop index '{QueryInfo.TableName}.{QueryInfo.TargetIndex}'{Environment.NewLine}{innerException.Message}");
            }

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
