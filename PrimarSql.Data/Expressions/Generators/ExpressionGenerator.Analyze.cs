using System;
using System.Linq;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models;
using PrimarSql.Data.Models.Conditions;

namespace PrimarSql.Data.Expressions.Generators
{
    internal partial class ExpressionGenerator
    {
        private static readonly string[] _sortKeyOperator = { "=", "<", "<=", ">", ">=" };

        private static readonly string[] _comparisonOperator = { "=", ">", "<", "<=", ">=", "<>", "!=", "<=>" };

        private ICondition AnalyzeInternal(IExpression expression, IExpression parent, int depth)
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
        private ICondition AnalyzeMultipleExpression(MultipleExpression expression, IExpression parent, int depth)
        {
            if (!(parent is InExpression))
                return AnalyzeNestedExpression(expression, parent, depth);

            return new MultipleCondition(
                expression.Expressions.Select(singleExpression => AnalyzeInternal(singleExpression, expression, depth))
            );
        }

        private ICondition AnalyzeNestedExpression(MultipleExpression expression, IExpression parent, int depth)
        {
            if (expression.Expressions.Length > 1)
                throw new InvalidOperationException("Too many expressions.");

            return new NestedCondition(AnalyzeInternal(expression.Expressions[0], expression, depth));
        }
        #endregion

        #region InExpression
        private ICondition AnalyzeInExpression(InExpression expression, IExpression parent, in int depth)
        {
            var leftCondition = AnalyzeInternal(expression.Target, expression, depth + 1);
            var rightCondition = AnalyzeInternal(expression.Sources, expression, depth + 1);

            return new OperatorCondition(leftCondition, "IN", rightCondition);
        }
        #endregion

        #region LogicalExpression
        private ICondition AnalyzeLogicalExpression(LogicalExpression expression, IExpression parent, int depth)
        {
            var left = expression.Left;
            var right = expression.Right;
            var @operator = expression.Operator;

            // check hash key or sort key
            if (depth == 0 && IsColumnAndLiteralExpression(left, right, out var target, out var value))
            {
                if (AnalyzeHashKey(target, value, @operator, out var hashKeyCondition))
                    return hashKeyCondition;

                if (AnalyzeSortKey(target, value, @operator, out var sortKeyCondition))
                    return sortKeyCondition;
            }

            switch (@operator.ToLower())
            {
                case "is":
                    return AnalyzeIsNullExpression(expression, parent, depth);
            }

            var leftCondition = AnalyzeInternal(left, expression, depth);
            var rightCondition = AnalyzeInternal(right, expression, depth);

            if (!(GetPrimaryKeyFromCondition(leftCondition, out var leftKey) && @operator.EqualsIgnore("AND")))
                Context.RemoveKey(leftKey);

            if (!(GetPrimaryKeyFromCondition(rightCondition, out var rightKey) && @operator.EqualsIgnore("AND")))
                Context.RemoveKey(rightKey);

            return new OperatorCondition(leftCondition, @operator, rightCondition);
        }

        // EXPR IS (NOT) NULL
        private ICondition AnalyzeIsNullExpression(LogicalExpression expression, IExpression _, int depth)
        {
            var left = expression.Left;
            var right = expression.Right;

            if (right is UnaryExpression unaryExpression && unaryExpression.Operator.EqualsIgnore("not"))
                right = unaryExpression.Expression;

            return new OperatorCondition(AnalyzeInternal(left, expression, depth), "=", AnalyzeInternal(right, expression, depth));
        }
        #endregion

        #region LiteralExpression
        private StringCondition AnalyzeLiteralExpression(LiteralExpression expression, IExpression parent, int _)
        {
            if (parent == null)
                throw new InvalidOperationException("Literal value cannot be used alone.");

            var valueName = Context.GetAttributeValue(expression.Value.ToAttributeValue());

            return new StringCondition(valueName.Key);
        }
        #endregion

        #region ColumnExpression
        private StringCondition AnalyzeColumnExpression(MemberExpression expression, IExpression parent, int depth)
        {
            if (parent == null)
                throw new InvalidOperationException("Literal value cannot be used alone.");

            return new StringCondition(string.Join(".", expression.Name));
        }
        #endregion

        #region Analyze HashKey / SortKey
        private bool AnalyzeHashKey(MemberExpression target, LiteralExpression value, string @operator, out HashKeyCondition hashKeyCondition)
        {
            hashKeyCondition = null;

            if (@operator != "=" || target.Name.Length > 1)
                return false;

            var targetName = target.Name[0];

            if (targetName != HashKeyName)
                return false;

            var attrName = Context.GetAttributeName(targetName);
            var attrValue = Context.GetAttributeValue(value.Value.ToAttributeValue());

            Context.Enter(attrName);
            Context.Enter(attrValue);

            hashKeyCondition = Context.AddHashKeyCondition(attrName, attrValue);

            return true;
        }

        private bool AnalyzeSortKey(MemberExpression target, LiteralExpression value, string @operator, out SortKeyCondition sortKeyCondition)
        {
            sortKeyCondition = null;

            if (target.Name.Length > 1 || !IsValidSortKeyOperator(@operator))
                return false;

            var targetName = target.Name[0];

            if (targetName != SortKeyName)
                return false;

            var attrName = Context.GetAttributeName(targetName);
            var attrValue = Context.GetAttributeValue(value.Value.ToAttributeValue());

            Context.Enter(attrName);
            Context.Enter(attrValue);

            sortKeyCondition = Context.AddSortKeyCondition(attrName, attrValue, null, @operator, SortKeyType.Comparison);
            
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

        private bool GetPrimaryKeyFromCondition(ICondition condition, out IKey key)
        {
            switch (condition)
            {
                case HashKeyCondition hashKeyCondition:
                    key = hashKeyCondition.HashKey;
                    return true;

                case SortKeyCondition sortKeyCondition:
                    key = sortKeyCondition.SortKey;
                    return true;

                default:
                    key = null;
                    return false;
            }
        }

        private bool IsValidSortKeyOperator(string @operator)
        {
            return _sortKeyOperator.Contains(@operator);
        }
        #endregion
    }
}
