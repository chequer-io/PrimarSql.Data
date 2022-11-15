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
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class InsertPlanner : QueryPlanner<InsertQueryInfo>
    {
        private string _hashKeyName = string.Empty;
        private string _sortKeyName = string.Empty;
        private PutItemRequest _request;
        private int _insertCount;

        public InsertPlanner(InsertQueryInfo queryInfo) : base(queryInfo)
        {
        }

        public override DbDataReader Execute()
        {
            return ExecuteAsync().GetResultSynchronously();
        }

        public override async Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            _insertCount = 0;
            var tableDescription = await Context.GetTableDescriptionAsync(QueryInfo.TableName, cancellationToken);
            List<KeySchemaElement> keySchema = tableDescription.KeySchema;
            _hashKeyName = keySchema.First(schema => schema.KeyType == KeyType.HASH).AttributeName;
            _sortKeyName = keySchema.FirstOrDefault(schema => schema.KeyType == KeyType.RANGE)?.AttributeName;

            if (QueryInfo.InsertValueType == InsertValueType.Subquery)
                throw new NotSupportedFeatureException("Insert from select feature is not supported yet.");

            switch (QueryInfo.InsertValueType)
            {
                case InsertValueType.Subquery:
                    throw new NotSupportedFeatureException("Insert from select feature is not supported yet.");

                case InsertValueType.RawValues:
                {
                    if (QueryInfo.Columns.Length == 0)
                        QueryInfo.Columns = keySchema.Select(schema => schema.AttributeName).ToArray();

                    if (!QueryInfo.Columns.Contains(_hashKeyName))
                        throw new NotSupportedFeatureException("The value must contain the hash key.");

                    foreach (IEnumerable<IExpression> row in QueryInfo.Rows)
                    {
                        if (await CallPutItemAsync(GetItemFromRawValue(row), cancellationToken))
                            _insertCount++;
                    }

                    break;
                }

                case InsertValueType.JsonValues:
                {
                    foreach (Dictionary<string, AttributeValue> row in QueryInfo.JsonValues)
                    {
                        if (await CallPutItemAsync(row, cancellationToken))
                            _insertCount++;
                    }

                    break;
                }
            }

            return new PrimarSqlDataReader(new EmptyDataProvider(_insertCount));
        }

        private Dictionary<string, AttributeValue> GetItemFromRawValue(IEnumerable<IExpression> row)
        {
            var item = new Dictionary<string, AttributeValue>();
            int i = 0;

            foreach (var cell in row)
            {
                if (i + 1 > QueryInfo.Columns.Length)
                    throw new ColumnValueCountExceedException();

                if (cell is LiteralExpression literalExpression)
                {
                    string column = QueryInfo.Columns[i];
                    var attrValue = literalExpression.Value.ToAttributeValue();

                    item[column] = attrValue;
                }
                else
                {
                    throw new NotSupportedFeatureException($"Not Support '{cell.GetType().Name}' type to insert value.");
                }

                i++;
            }

            if (item.Count != QueryInfo.Columns.Length)
                throw new ColumnValueCountDifferentException();

            return item;
        }

        private async Task<bool> CallPutItemAsync(Dictionary<string, AttributeValue> item, CancellationToken cancellationToken)
        {
            _request = new PutItemRequest
            {
                TableName = QueryInfo.TableName,
                Item = item
            };

            SetDuplicateCheckExpression(_request, item);

            try
            {
                await Context.Client.PutItemAsync(_request, cancellationToken);
                return true;
            }
            catch (PrimarSqlException e) when (e.Error is PrimarSqlError.ConditionalCheckFailed && QueryInfo.IgnoreDuplicate)
            {
                // ignore duplicate
                return false;
            }
            catch (Exception e) when (e is not PrimarSqlException)
            {
                throw new PrimarSqlException(
                    PrimarSqlError.Unknown,
                    $"Error while insert item (Count: {_insertCount}){Environment.NewLine}{e.Message}"
                );
            }
        }

        private void SetDuplicateCheckExpression(PutItemRequest request, Dictionary<string, AttributeValue> item)
        {
            ExpressionAttributeNames.Clear();
            ExpressionAttributeValues.Clear();
            AttributeGenerator.ResetIndex();

            var hashKeyAttrValue = item.FirstOrDefault(x => x.Key == _hashKeyName).Value;

            if (hashKeyAttrValue == null)
                throw new PrimarSqlException(PrimarSqlError.Syntax, $"Item does not contains Hash key: {_hashKeyName}");

            var sb = new StringBuilder();

            var hashKeyName = GetAttributeName(_hashKeyName);
            var hashKeyValue = GetAttributeValue(hashKeyAttrValue);

            sb.Append($"{hashKeyName} <> {hashKeyValue}");

            if (!string.IsNullOrEmpty(_sortKeyName))
            {
                var sortKeyAttrValue = item.FirstOrDefault(x => x.Key == _sortKeyName).Value;

                var sortKeyName = GetAttributeName(_sortKeyName);
                var sortKeyValue = GetAttributeValue(sortKeyAttrValue);

                sb.Append($" AND {sortKeyName} <> {sortKeyValue}");
            }

            request.ExpressionAttributeNames = ExpressionAttributeNames;
            request.ExpressionAttributeValues = ExpressionAttributeValues;
            request.ConditionExpression = sb.ToString();
        }
    }
}
