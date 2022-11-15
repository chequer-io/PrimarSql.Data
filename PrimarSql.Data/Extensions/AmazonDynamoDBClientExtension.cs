using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;

namespace PrimarSql.Data.Extensions
{
    internal static class AmazonDynamoDBClientExtension
    {
        public static async ValueTask WaitForTableDeletingAsync(this IAmazonDynamoDB client, string tableName, CancellationToken cancellationToken = default)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var response = await client.DescribeTableAsync(tableName, cancellationToken);
                    var tableStatus = response.Table.TableStatus;

                    if (tableStatus != TableStatus.DELETING)
                        throw new PrimarSqlException(PrimarSqlError.Unknown, $"Unexpected table status: {tableStatus.Value}");
                }
                catch (PrimarSqlException e) when (e.Error is PrimarSqlError.ResourceNotFound)
                {
                    break;
                }

                await Task.Delay(300, cancellationToken);
            }
        }

        public static ValueTask WaitForTableCreatingAsync(this IAmazonDynamoDB client, string tableName, CancellationToken cancellationToken = default)
        {
            return WaitForTableStatusIsActiveAsync(client, tableName, new[] { TableStatus.CREATING }, cancellationToken);
        }

        public static ValueTask WaitForTableUpdatingAsync(this IAmazonDynamoDB client, string tableName, CancellationToken cancellationToken = default)
        {
            return WaitForTableStatusIsActiveAsync(client, tableName, new[] { TableStatus.UPDATING }, cancellationToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static async ValueTask WaitForTableStatusIsActiveAsync(
            IAmazonDynamoDB client,
            string tableName,
            TableStatus[] expectedTableStatuses,
            CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var response = await client.DescribeTableAsync(tableName, cancellationToken);
                var tableStatus = response.Table.TableStatus;

                if (tableStatus == TableStatus.ACTIVE)
                    break;

                if (!expectedTableStatuses.Contains(tableStatus))
                {
                    var expected = string.Join(", ", expectedTableStatuses.Select(x => x.Value));
                    throw new PrimarSqlException(PrimarSqlError.Unknown, $"Unexpected table status: {tableStatus.Value} (Expected: {expected})");
                }

                await Task.Delay(300, cancellationToken);
            }
        }
    }
}
