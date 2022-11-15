using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Exceptions;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Expressions.Generators
{
    internal partial class ExpressionGenerator
    {
        public TableDescription TableDescription { get; }

        public string IndexName { get; }

        public IExpression Expression { get; }

        public string HashKeyName { get; private set; }

        public string SortKeyName { get; private set; }

        protected GeneratorContext Context;

        public ExpressionGenerator(TableDescription tableDescription, string indexName, IExpression expression)
        {
            TableDescription = tableDescription;
            IndexName = indexName;
            Expression = expression;
        }

        public ExpressionGenerateResult Analyze()
        {
            Context = new GeneratorContext();

            SetPrimaryKey();
            var valueManager = new AttributeValueManager();
            var condition = AnalyzeInternal(Expression, null, 0);
            ValidatePrimaryKey();

            return new ExpressionGenerateResult
            {
                HashKey = Context.HashKeys.FirstOrDefault().Key,
                SortKey = Context.SortKeys.FirstOrDefault().Key,
                FilterExpression = condition?.ToExpression(valueManager),
                ExpressionAttributeNames = Context.AttributeNames.ToArray(),
                ExpressionAttributeValues = Context.AttributeValues.ToArray()
            };
        }

        private List<KeySchemaElement> FindLocalSecondaryIndex(string indexName)
        {
            return TableDescription
                .LocalSecondaryIndexes
                .FirstOrDefault(index => index.IndexName == indexName)?
                .KeySchema;
        }

        private List<KeySchemaElement> FindGlobalSecondaryIndex(string indexName)
        {
            return TableDescription
                .GlobalSecondaryIndexes
                .FirstOrDefault(index => index.IndexName == indexName)?
                .KeySchema;
        }

        private void SetPrimaryKey()
        {
            List<KeySchemaElement> keySchemaElements;

            if (string.IsNullOrEmpty(IndexName))
            {
                keySchemaElements = TableDescription.KeySchema;
            }
            else
            {
                keySchemaElements =
                    FindLocalSecondaryIndex(IndexName) ??
                    FindGlobalSecondaryIndex(IndexName) ??
                    throw new PrimarSqlException(PrimarSqlError.Syntax, $"{TableDescription.TableName} table has no '{IndexName}' Index.");
            }

            foreach (var element in keySchemaElements)
            {
                if (element.KeyType == KeyType.HASH)
                {
                    HashKeyName = element.AttributeName;
                }
                else if (element.KeyType == KeyType.RANGE)
                {
                    SortKeyName = element.AttributeName;
                }
                else
                {
                    throw new NotSupportedFeatureException($"KeyType '{element.KeyType.Value}' is not supported.");
                }
            }
        }

        private void ValidatePrimaryKey()
        {
            if (Context.HashKeys.Count > 1)
                Context.HashKeys.Clear();

            if (Context.SortKeys.Count > 1 || Context.SortKeys.Count == 1 && Context.HashKeys.Count != 1)
                Context.SortKeys.Clear();

            if (Context.HashKeys.Count == 1)
            {
                var hashKey = Context.HashKeys.FirstOrDefault();
                hashKey.Value.IsActivated = false;
            }

            if (Context.SortKeys.Count == 1)
            {
                var sortKey = Context.SortKeys.FirstOrDefault();
                sortKey.Value.IsActivated = false;
            }
        }
    }
}
