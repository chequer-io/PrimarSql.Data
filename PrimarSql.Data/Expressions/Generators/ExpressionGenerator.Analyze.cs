using System;
using System.Linq;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models;
using PrimarSql.Data.Models.ExpressionBuffers;

namespace PrimarSql.Data.Expressions.Generators
{
    internal partial class ExpressionGenerator
    {
        private static readonly string[] _sortKeyOperator = { "=", "<", "<=", ">", ">=" };

        private static readonly string[] _comparisonOperator = { "=", ">", "<", "<=", ">=", "<>", "!=", "<=>" };

        private AnalyzeResult AnalyzeInternal(IExpression expression, IExpression parent, int depth)
        {
            switch (expression)
            {
                case MultipleExpression multipleExpression:
                    return AnalyzeMultipleExpression(multipleExpression, parent, depth);

                case InExpression inExpression:
                    return AnalyzeInExpression(inExpression, parent, depth);

                case LogicalExpression logicalExpression:
                    return AnalyzeLogicalExpression(logicalExpression, parent, depth);

                case LiteralExpression literalExpression:
                    return AnalyzeLiteralExpression(literalExpression, parent, depth);

                case MemberExpression columnExpression:
                    return AnalyzeColumnExpression(columnExpression, parent, depth);
                
                case SelectExpression selectExpression:
                    throw new NotSupportedException("Not Supported Subquery Expression Feature.");
            }

            throw new NotSupportedException($"Not Supported {expression?.GetType().Name ?? "Unknown Type"}.");
        }

        #region MultipleExpression
        private AnalyzeResult AnalyzeMultipleExpression(MultipleExpression expression, IExpression parent, int depth)
        {
            if (!(parent is InExpression))
                return AnalyzeNestedExpression(expression, parent, depth);

            Context.Append("(");

            for (int i = 0; i < expression.Expressions.Length; i++)
            {
                var singleExpression = expression.Expressions[i];
                AnalyzeInternal(singleExpression, expression, depth);

                if (i != expression.Expressions.Length - 1)
                    Context.Append(",");
            }

            Context.Append(")");

            return AnalyzeResult.Success;
        }

        private AnalyzeResult AnalyzeNestedExpression(MultipleExpression expression, IExpression parent, int depth)
        {
            if (expression.Expressions.Length > 1)
                throw new InvalidOperationException("Too many expressions.");

            Context.Append("(");
            var result = AnalyzeInternal(expression.Expressions[0], expression, depth);
            Context.Append(")");

            if (IsHashOrSortKey(result))
            {
                result.Key.StartToken -= 1;
                result.Key.EndToken += 1;

                return result;
            }

            return AnalyzeResult.Success;
        }
        #endregion

        #region InExpression
        private AnalyzeResult AnalyzeInExpression(InExpression expression, IExpression parent, in int depth)
        {
            AnalyzeInternal(expression.Target, expression, depth + 1);
            Context.Append("IN");
            AnalyzeInternal(expression.Sources, expression, depth + 1);

            return AnalyzeResult.Success;
        }
        #endregion

        #region LogicalExpression
        private AnalyzeResult AnalyzeLogicalExpression(LogicalExpression expression, IExpression parent, int depth)
        {
            var left = expression.Left;
            var right = expression.Right;
            var @operator = expression.Operator;
            
            // check hash key or sort key
            if (depth == 0 && IsColumnAndLiteralExpression(left, right, out var target, out var value))
            {
                if (AnalyzeHashKey(target, value, @operator, out var hashKey))
                    return new AnalyzeResult(hashKey);

                if (AnalyzeSortKey(target, value, @operator, out var sortKey))
                    return new AnalyzeResult(sortKey);
            }

            switch (@operator.ToLower())
            {
                case "is":
                    return AnalyzeIsNullExpression(expression, parent, depth); 
            }
            
            // left
            var leftResult = AnalyzeInternal(left, expression, depth);

            if (IsHashOrSortKey(leftResult) && @operator.Equals("AND", StringComparison.OrdinalIgnoreCase))
                leftResult.Key.EndToken = Context.BufferIndex + 1;
            else
                Context.RemoveKey(leftResult.Key);

            // operator
            Context.Append(@operator);
 
            // right
            var rightResult = AnalyzeInternal(right, expression, depth);

            if (IsHashOrSortKey(rightResult) && @operator.Equals("AND", StringComparison.OrdinalIgnoreCase))
                rightResult.Key.StartToken = Context.BufferIndex - 1;
            else
                Context.RemoveKey(rightResult.Key);

            return AnalyzeResult.Success;
        }

        // EXPR IS (NOT) NULL
        private AnalyzeResult AnalyzeIsNullExpression(LogicalExpression expression, IExpression parent, int depth)
        {
            var left = expression.Left;
            var right = expression.Right;
            var @operator = expression.Operator;

            if (right is UnaryExpression unaryExpression && unaryExpression.Operator.Equals("not", StringComparison.OrdinalIgnoreCase))
            {
                right = unaryExpression.Expression;
            }
            
            AnalyzeInternal(left, expression, depth);
            Context.Append("=");
            AnalyzeInternal(right, expression, depth);
            
            return AnalyzeResult.Success;
        }
        #endregion
        
        #region LiteralExpression
        private AnalyzeResult AnalyzeLiteralExpression(LiteralExpression expression, IExpression parent, int depth)
        {
            if (parent == null)
                throw new InvalidOperationException("Literal value cannot be used alone.");

            var valueName = Context.GetAttributeValue(expression.Value.ToAttributeValue());
            Context.Append(valueName.Key);

            return AnalyzeResult.Success;
        }
        #endregion
        
        #region ColumnExpression
        private AnalyzeResult AnalyzeColumnExpression(MemberExpression expression, IExpression parent, int depth)
        {
            if (parent == null)
                throw new InvalidOperationException("Literal value cannot be used alone.");

            Context.Append(string.Join(".", expression.Name));
            
            return AnalyzeResult.Success;
        }
        #endregion
        
        #region Analyze HashKey / SortKey
        private bool AnalyzeHashKey(MemberExpression target, LiteralExpression value, string @operator, out HashKey hashKey)
        {
            hashKey = null;

            if (@operator != "=" || target.Name.Length > 1)
                return false;

            var targetName = target.Name[0];

            if (targetName != HashKeyName)
                return false;

            var attrName = Context.GetAttributeName(targetName);
            var attrValue = Context.GetAttributeValue(value.Value.ToAttributeValue());

            Context.Enter(attrName);
            Context.Enter(attrValue);
            
            hashKey = Context.AddHashKey(attrName, attrValue);

            Context.Append($"{attrName.Key} = {attrValue.Key}");
            hashKey.StartToken = Context.BufferIndex;
            hashKey.EndToken = Context.BufferIndex;

            return true;
        }

        private bool AnalyzeSortKey(MemberExpression target, LiteralExpression value, string @operator, out SortKey sortKey)
        {
            sortKey = null;

            if (target.Name.Length > 1 || !IsValidSortKeyOperator(@operator))
                return false;

            var targetName = target.Name[0];

            if (targetName != SortKeyName)
                return false;

            var attrName = Context.GetAttributeName(targetName);
            var attrValue = Context.GetAttributeValue(value.Value.ToAttributeValue());

            Context.Enter(attrName);
            Context.Enter(attrValue);

            sortKey = Context.AddSortKey(attrName, attrValue, null, @operator, SortKeyType.Comparison);

            Context.Append($"{attrName} {@operator} {attrValue}");
            sortKey.StartToken = Context.BufferIndex;
            sortKey.EndToken = Context.BufferIndex;
            
            return true;
        }
        #endregion

        #region Check Conditions
        private bool IsColumnAndLiteralExpression(IExpression left, IExpression right, out MemberExpression target, out LiteralExpression value)
        {
            target = null;
            value = null;

            if (left is MemberExpression lc && right is LiteralExpression rl)
            {
                target = lc;
                value = rl;
                return true;
            }

            if (right is MemberExpression rc && left is LiteralExpression ll)
            {
                target = rc;
                value = ll;
                return true;
            }

            return false;
        }

        private bool IsHashOrSortKey(AnalyzeResult result)
        {
            return result.State == AnalyzeState.HashKey || result.State == AnalyzeState.SortKey;
        }

        private bool IsValidSortKeyOperator(string @operator)
        {
            return _sortKeyOperator.Contains(@operator);
        }
        #endregion
    }
}
