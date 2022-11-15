using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class DropIndexPlanner : QueryPlanner<DropIndexQueryInfo>
    {
        public override DbDataReader Execute()
        {
            return ExecuteAsync().GetResultSynchronously();
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

                var response = await Context.Client.UpdateTableAsync(request, cancellationToken);

                if (response.TableDescription.TableStatus == TableStatus.UPDATING)
                    await Context.Client.WaitForTableUpdatingAsync(QueryInfo.TableName, cancellationToken);
            }
            catch (Exception e) when (e is not PrimarSqlException)
            {
                throw new PrimarSqlException(
                    PrimarSqlError.Unknown,
                    $"Error while drop index '{QueryInfo.TableName}.{QueryInfo.TargetIndex}'{Environment.NewLine}{e.Message}"
                );
            }

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
