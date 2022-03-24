using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
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
                return ExecuteAsync().Result;
            }
            catch (AggregateException e) when (e.InnerExceptions.Count == 1)
            {
                throw e.InnerExceptions[0];
            }
        }

        public override async Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
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

                await Context.Client.UpdateTableAsync(request, cancellationToken);
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
