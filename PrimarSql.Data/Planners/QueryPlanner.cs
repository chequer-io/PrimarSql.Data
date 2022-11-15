using System;
using System.Collections.Generic;
using System.Data.Common;
using Amazon.DynamoDBv2;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Exceptions;
using PrimarSql.Data.Expressions.Generators;
using PrimarSql.Data.Models;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Planners
{
    internal abstract class QueryPlanner<T> : IQueryPlanner where T : IQueryInfo
    {
        protected Dictionary<string, string> ExpressionAttributeNames { get; }

        protected Dictionary<string, AttributeValue> ExpressionAttributeValues { get; }

        public ExpressionAttributeGenerator AttributeGenerator { get; }

        protected QueryPlanner()
        {
            ExpressionAttributeNames = new Dictionary<string, string>();
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>();
            AttributeGenerator = new ExpressionAttributeGenerator();
        }

        protected QueryPlanner(T queryInfo) : this()
        {
            QueryInfo = queryInfo;
        }

        protected string GetAttributeName(string rawName)
        {
            var attributeName = AttributeGenerator.GetAttributeName(rawName);
            ExpressionAttributeNames[attributeName.Key] = attributeName.Value;

            return attributeName.Key;
        }

        protected string GetAttributeName(IEnumerable<IPart> rawName)
        {
            var sb = new StringBuilder();

            foreach (var part in rawName)
            {
                switch (part)
                {
                    case IdentifierPart identifierPart:
                        if (sb.Length != 0)
                            sb.Append(".");

                        sb.Append(GetAttributeName(identifierPart.Identifier));
                        break;

                    case IndexPart indexPart:
                        sb.Append($"[{indexPart.Index}]");
                        break;
                }
            }

            return sb.ToString();
        }

        protected string GetAttributeValue(AttributeValue rawValue)
        {
            var attributeValue = AttributeGenerator.GetAttributeValue(rawValue);
            ExpressionAttributeValues[attributeValue.Key] = attributeValue.Value;

            return attributeValue.Key;
        }

        protected ScalarAttributeType DataTypeToScalarAttributeType(string dataType)
        {
            dataType = dataType.ToLower();

            if (VisitorHelper.StringTypes.Contains(dataType))
                return ScalarAttributeType.S;

            if (VisitorHelper.NumberTypes.Contains(dataType))
                return ScalarAttributeType.N;

            throw new NotSupportedFeatureException($"{dataType.ToUpper()} cannot use to Index Type.");
        }

        IQueryInfo IQueryPlanner.QueryInfo => QueryInfo;

        public T QueryInfo { get; set; }

        public QueryContext Context { get; set; }

        public abstract DbDataReader Execute();

        public abstract Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default);
    }
}
