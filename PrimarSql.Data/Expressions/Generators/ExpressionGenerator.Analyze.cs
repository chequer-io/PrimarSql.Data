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
            if (expression == null)
                return null;

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

                case FunctionExpression functionExpression:
                    return AnalyzeFunctionExpression(functionExpression, parent, depth);

                case BetweenExpression betweenExpression:
                    return AnalyzeBetweenExpression(betweenExpression, parent, depth);

                case UnaryExpression unaryExpression:
                    return AnalyzeUnaryExpression(unaryExpression, parent, depth);
            }

            throw new NotSupportedException($"Not Supported {expression.GetType().Name}.");
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

            ICondition condition = new OperatorCondition(leftCondition, "IN", rightCondition);

            return !expression.IsNot
                ? condition
                : CreateNotCondition(condition);
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

            if (!(GetPrimaryKeyFromCondition(leftCondition, out var leftKey) && (@operator.EqualsIgnore("AND") || @operator.Equals("&&"))))
                Context.RemoveKey(leftKey);

            if (!(GetPrimaryKeyFromCondition(rightCondition, out var rightKey) && (@operator.EqualsIgnore("AND") || @operator.Equals("&&"))))
                Context.RemoveKey(rightKey);

            return new OperatorCondition(leftCondition, @operator, rightCondition);
        }

        // EXPR IS (NOT) NULL
        private ICondition AnalyzeIsNullExpression(LogicalExpression expression, IExpression _, int depth)
        {
            var left = expression.Left;
            var right = expression.Right;
            var isNot = false;

            if (right is UnaryExpression unaryExpression && unaryExpression.Operator.EqualsIgnore("not"))
            {
                isNot = true;
                right = unaryExpression.Expression;
            }

            return new OperatorCondition(AnalyzeInternal(left, expression, depth), isNot ? "=" : "<>", AnalyzeInternal(right, expression, depth));
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

            var columnName = Context.GetAttributeName(expression.Name.ToName());

            return new StringCondition(columnName.Key);
        }
        #endregion

        #region Function
        private FunctionCondition AnalyzeFunctionExpression(FunctionExpression expression, IExpression parent, int depth)
        {
            if (!(expression.Member is MemberExpression memberExpression))
                throw new InvalidOperationException("Function member must MemberExpression.");

            return new FunctionCondition
            {
                MemberName = memberExpression.Name.ToName(),
                Parameters = expression.Parameters.Select(p => AnalyzeInternal(p, expression, depth + 1)).ToArray()
            };
        }
        #endregion

        #region Between
        private ICondition AnalyzeBetweenExpression(BetweenExpression expression, IExpression parent, int depth)
        {
            var condition = new BetweenCondition
            {
                Predicate = AnalyzeInternal(expression.Target, expression, depth + 1),
                Expression1 = AnalyzeInternal(expression.Expression1, expression, depth + 1),
                Expression2 = AnalyzeInternal(expression.Expression2, expression, depth + 1)
            };

            return !expression.IsNot
                ? condition
                : CreateNotCondition(condition);
        }
        #endregion

        #region Unary
        private ICondition AnalyzeUnaryExpression(UnaryExpression expression, IExpression parent, int depth)
        {
            return new UnaryCondition
            {
                Condition = AnalyzeInternal(expression.Expression, expression, depth + 1),
                Operator = expression.Operator
            };
        }
        #endregion

        private ICondition CreateNotCondition(ICondition condition)
        {
            return new UnaryCondition
            {
                Condition = condition,
                Operator = "NOT"
            };
        }

        #region Analyze HashKey / SortKey
        private bool AnalyzeHashKey(MemberExpression target, LiteralExpression value, string @operator, out HashKeyCondition hashKeyCondition)
        {
            hashKeyCondition = null;

            if (@operator != "=" || target.Name.Length > 1)
                return false;

            var targetName = target.Name[0].ToString();

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

            var targetName = target.Name[0].ToString();

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
