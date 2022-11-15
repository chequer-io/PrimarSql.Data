using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Exceptions;
using PrimarSql.Data.Expressions;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Providers;
using PrimarSql.Data.Sources;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Planners
{
    internal sealed class UpdatePlanner : QueryPlanner<UpdateQueryInfo>
    {
        public UpdatePlanner(UpdateQueryInfo queryInfo)
        {
            QueryInfo = queryInfo;
        }

        public async Task<IColumn[]> GetColumnsAsync(CancellationToken cancellationToken = default)
        {
            var columns = new List<IColumn>();

            var tableDescription = await Context.GetTableDescriptionAsync(QueryInfo.TableName, cancellationToken);
            var hashKeyName = tableDescription.KeySchema.First(schema => schema.KeyType == KeyType.HASH).AttributeName;
            var sortKeyName = tableDescription.KeySchema.FirstOrDefault(schema => schema.KeyType == KeyType.RANGE)?.AttributeName;

            columns.Add(new PropertyColumn(hashKeyName));

            if (!string.IsNullOrEmpty(sortKeyName))
                columns.Add(new PropertyColumn(sortKeyName));

            return columns.ToArray();
        }

        private string GetUpdateExpression()
        {
            if (!QueryInfo.UpdatedElements.Any())
                return string.Empty;

            bool isRemove;

            if (QueryInfo.UpdatedElements.All(x => x.IsRemove))
                isRemove = true;
            else if (QueryInfo.UpdatedElements.All(x => !x.IsRemove))
                isRemove = false;
            else
                throw new PrimarSqlException(PrimarSqlError.Syntax, "Update element cannot be different type between elements.");

            var sb = new StringBuilder(isRemove ? "REMOVE" : "SET");

            foreach (var element in QueryInfo.UpdatedElements)
            {
                var name = GetAttributeName(element.Name);

                if (isRemove)
                {
                    sb.Append($" {name},");
                }
                else
                {
                    sb.Append($" {name} = {GetValue(element.Value, name)},");
                }
            }

            sb.Remove(sb.Length - 1, 1);
            return sb.ToString();
        }

        private string GetValue(IExpression expression, string name = null)
        {
            AttributeValue value;

            // TODO: Support function expression for UpdateExpression
            switch (expression)
            {
                case LiteralExpression literalExpression:
                    value = literalExpression.Value.ToAttributeValue();
                    break;

                case MultipleExpression multipleExpression:
                    IEnumerable<string> nonLiteralExpressions = multipleExpression.Expressions
                        .Where(x => x is not LiteralExpression)
                        .Select(x => x.GetType().Name)
                        .Distinct();

                    if (nonLiteralExpressions.Any())
                    {
                        string expressions = string.Join(", ", nonLiteralExpressions.Select(x => $"'{x}'"));
                        throw new NotSupportedFeatureException($"Not supported expressions {expressions} in update");
                    }

                    value = multipleExpression.Expressions
                        .Cast<LiteralExpression>()
                        .Select(x => x.Value.ToAttributeValue()).ToAttributeValue();

                    break;

                case ArrayAppendExpression arrayAppendExpression:
                    return $"list_append({name}, {GetValue(arrayAppendExpression.AppendItem)})";

                default:
                    throw new NotSupportedFeatureException($"Not supported expression '{expression.GetType().Name}'.");
            }

            return GetAttributeValue(value);
        }

        public override DbDataReader Execute()
        {
            return ExecuteAsync().GetResultSynchronously();
        }

        public override async Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            int updatedCount = 0;

            var selectQueryInfo = new SelectQueryInfo
            {
                WhereExpression = QueryInfo.WhereExpression,
                Limit = QueryInfo.Limit,
                TableSource = new AtomTableSource(QueryInfo.TableName, string.Empty),
                Columns = await GetColumnsAsync(cancellationToken),
            };

            // TODO: Performance issue, need to performance enhancement.
            if (QueryInfo.WhereExpression == null)
                throw new PrimarSqlException(PrimarSqlError.Syntax, "Update not support without where expression.");

            var planner = new SelectPlanner
            {
                QueryInfo = selectQueryInfo,
                Context = Context
            };

            var reader = await planner.ExecuteAsync(cancellationToken);
            bool hasSortKey = selectQueryInfo.Columns.Length == 2;
            string updateExpression = GetUpdateExpression();

            while (await reader.ReadAsync(cancellationToken))
            {
                var request = new UpdateItemRequest
                {
                    TableName = QueryInfo.TableName,
                    UpdateExpression = updateExpression,
                    ExpressionAttributeNames = ExpressionAttributeNames,
                    ExpressionAttributeValues = ExpressionAttributeValues
                };

                request.Key.Add(IdentifierUtility.Unescape(reader.GetName(0)), reader[0].ToAttributeValue());

                if (hasSortKey)
                    request.Key.Add(IdentifierUtility.Unescape(reader.GetName(1)), reader[1].ToAttributeValue());

                try
                {
                    await Context.Client.UpdateItemAsync(request, cancellationToken);
                }
                catch (Exception e) when (e is not PrimarSqlException)
                {
                    throw new PrimarSqlException(
                        PrimarSqlError.Unknown,
                        $"Error while update Item (Key: {reader.GetName(0)}){Environment.NewLine}{e.Message}"
                    );
                }

                updatedCount++;
            }

            return new PrimarSqlDataReader(new EmptyDataProvider(updatedCount));
        }
    }
}
