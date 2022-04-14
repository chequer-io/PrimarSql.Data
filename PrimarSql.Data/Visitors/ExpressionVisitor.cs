﻿using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json.Linq;
using PrimarSql.Data.Expressions;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models;
using PrimarSql.Data.Utilities;
using static PrimarSql.Internal.PrimarSqlParser;

namespace PrimarSql.Data.Visitors
{
    internal static class ExpressionVisitor
    {
        public static IExpression VisitExpression(ExpressionContext context)
        {
            switch (context)
            {
                case NotExpressionContext notExpressionContext:
                    return VisitNotExpression(notExpressionContext);

                case LogicalExpressionContext logicalExpressionContext:
                    return VisitLogicalExpression(logicalExpressionContext);

                case PredicateExpressionContext predicateExpressionContext:
                    return VisitPredicateExpression(predicateExpressionContext);
            }

            return null;
        }

        #region JsonObject
        public static Dictionary<string, AttributeValue> VisitJsonObject(JsonObjectContext context)
        {
            return context.jsonValuePair()
                .Select(VisitJsonValuePair)
                .ToDictionary(kv => kv.Key, kv => kv.Value);
        }

        public static KeyValuePair<string, AttributeValue> VisitJsonValuePair(JsonValuePairContext context)
        {
            var key = context.stringLiteral().GetText()[1..^1];

            return new KeyValuePair<string, AttributeValue>(key, VisitJsonValue(context.jsonValue()));
        }

        public static AttributeValue VisitJsonValue(JsonValueContext context)
        {
            return JToken.Parse(context.GetText()).ToAttributeValue();
        }
        #endregion

        public static UnaryExpression VisitNotExpression(NotExpressionContext notExpressionContext)
        {
            return new UnaryExpression
            {
                Expression = VisitExpression(notExpressionContext.expression()),
                Operator = notExpressionContext.notOperator.Text
            };
        }

        public static LogicalExpression VisitLogicalExpression(LogicalExpressionContext context)
        {
            return new LogicalExpression
            {
                Left = VisitExpression(context.left),
                Operator = context.logicalOperator().GetText(),
                Right = VisitExpression(context.right),
            };
        }

        public static IExpression VisitPredicateExpression(PredicateExpressionContext context)
        {
            return VisitPredicate(context.predicate());
        }

        public static IExpression VisitPredicate(PredicateContext context)
        {
            switch (context)
            {
                case InPredicateContext inPredicateContext:
                    return VisitInPredicate(inPredicateContext);

                case IsNullPredicateContext isNullPredicateContext:
                    return VisitIsNullPredicate(isNullPredicateContext);

                case BinaryComparisonPredicateContext binaryComparisonPredicateContext:
                    return VisitBinaryComparasionPredicate(binaryComparisonPredicateContext);

                case BetweenPredicateContext betweenPredicateContext:
                    return VisitBetweenPredicate(betweenPredicateContext);

                case LikePredicateContext likePredicateContext:
                    return VisitLikePredicate(likePredicateContext);

                case RegexpPredicateContext regexpPredicateContext:
                    return VisitRegexpPredicate(regexpPredicateContext);

                case ExpressionAtomPredicateContext expressionAtomPredicateContext:
                    return VisitExpressionAtom(expressionAtomPredicateContext.expressionAtom());
            }

            return null;
        }

        public static InExpression VisitInPredicate(InPredicateContext context)
        {
            return new InExpression
            {
                IsNot = context.NOT() != null,
                Target = VisitPredicate(context.predicate()),
                Sources = VisitExpressions(context.expressions())
            };
        }

        public static LogicalExpression VisitIsNullPredicate(IsNullPredicateContext context)
        {
            var nullNotnull = context.nullNotnull();

            IExpression value = new LiteralExpression
            {
                Value = DBNull.Value,
                ValueType = LiteralValueType.Null
            };

            if (nullNotnull.NOT() != null)
            {
                value = new UnaryExpression
                {
                    Operator = "NOT",
                    Expression = value
                };
            }

            return new LogicalExpression
            {
                Left = VisitPredicate(context.predicate()),
                Operator = "IS",
                Right = value,
            };
        }

        public static IExpression VisitBinaryComparasionPredicate(BinaryComparisonPredicateContext context)
        {
            return new LogicalExpression
            {
                Left = VisitPredicate(context.left),
                Operator = context.comparisonOperator().GetText(),
                Right = VisitPredicate(context.right),
            };
        }

        public static IExpression VisitExpressionAtom(ExpressionAtomContext context)
        {
            switch (context)
            {
                case ConstantExpressionAtomContext constantExpressionAtomContext:
                    return VisitConstant(constantExpressionAtomContext.constant());

                case JsonExpressionAtomContext jsonExpressionAtomContext:
                    return new LiteralExpression
                    {
                        Value = JObject.Parse(jsonExpressionAtomContext.jsonObject().GetText()),
                        ValueType = LiteralValueType.Object
                    };

                case BinaryExpressionAtomContext binaryExpressionAtomContext:
                {
                    var base64Str = VisitStringLiteral(binaryExpressionAtomContext.stringLiteral()).Value.ToString();

                    return new LiteralExpression
                    {
                        Value = Convert.FromBase64String(base64Str),
                        ValueType = LiteralValueType.Binary
                    };
                }

                case FullColumnNameExpressionAtomContext fullColumnNameExpressionAtomContext:
                    return VisitFullColumnNameExpressionAtom(fullColumnNameExpressionAtomContext);

                case FunctionCallExpressionAtomContext functionCallExpressionAtomContext:
                    return VisitFunctionCall(functionCallExpressionAtomContext.functionCall());

                case NestedExpressionAtomContext nestedExpressionAtomContext:
                    return VisitNestedExpressionAtom(nestedExpressionAtomContext);

                case BitExpressionAtomContext bitExpressionAtomContext:
                    return new LogicalExpression
                    {
                        Left = VisitExpressionAtom(bitExpressionAtomContext.left),
                        Operator = bitExpressionAtomContext.bitOperator().GetText(),
                        Right = VisitExpressionAtom(bitExpressionAtomContext.right),
                    };

                case MathExpressionAtomContext mathExpressionAtomContext:
                    return new LogicalExpression
                    {
                        Left = VisitExpressionAtom(mathExpressionAtomContext.left),
                        Operator = mathExpressionAtomContext.mathOperator().GetText(),
                        Right = VisitExpressionAtom(mathExpressionAtomContext.right),
                    };
            }

            return null;
        }

        public static BetweenExpression VisitBetweenPredicate(BetweenPredicateContext context)
        {
            return new BetweenExpression
            {
                Target = VisitPredicate(context.predicate(0)),
                IsNot = context.NOT() != null,
                Expression1 = VisitPredicate(context.predicate(1)),
                Expression2 = VisitPredicate(context.predicate(2)),
            };
        }

        public static LogicalExpression VisitLikePredicate(LikePredicateContext context)
        {
            return new LogicalExpression
            {
                Left = VisitPredicate(context.left),
                Operator = context.NOT() != null ? "NOT LIKE" : "LIKE",
                Right = VisitPredicate(context.right)
            };
        }

        public static LogicalExpression VisitRegexpPredicate(RegexpPredicateContext context)
        {
            string @operator = string.Empty;

            if (context.NOT() != null)
                @operator = "NOT ";

            @operator += context.regex.Text.ToUpper();

            return new LogicalExpression
            {
                Left = VisitPredicate(context.left),
                Operator = @operator,
                Right = VisitPredicate(context.right)
            };
        }

        public static LiteralExpression VisitConstant(ConstantContext context)
        {
            switch (context)
            {
                case StringLiteralConstantContext stringLiteralConstantContext:
                    return VisitStringLiteral(stringLiteralConstantContext.stringLiteral());

                case PositiveDecimalLiteralConstantContext positiveDecimalLiteralConstantContext:
                    return new LiteralExpression
                    {
                        Value = long.Parse(positiveDecimalLiteralConstantContext.decimalLiteral().GetText()),
                        ValueType = LiteralValueType.Numeric
                    };

                case NegativeDecimalLiteralConstantContext negativeDecimalLiteralConstantContext:
                    return new LiteralExpression
                    {
                        Value = long.Parse("-" + negativeDecimalLiteralConstantContext.decimalLiteral().GetText()),
                        ValueType = LiteralValueType.Numeric
                    };

                case BooleanLiteralConstantContext booleanLiteralConstantContext:
                    return new LiteralExpression
                    {
                        Value = bool.Parse(booleanLiteralConstantContext.GetText()),
                        ValueType = LiteralValueType.Boolean
                    };

                case RealLiteralConstantContext realLiteralConstantContext:
                    return new LiteralExpression
                    {
                        Value = decimal.Parse(realLiteralConstantContext.GetText()),
                        ValueType = LiteralValueType.Numeric
                    };

                case NullConstantContext _:
                    return new LiteralExpression
                    {
                        Value = null,
                        ValueType = LiteralValueType.Null,
                    };

                default:
                    throw new NotSupportedException($"'{context.GetType().Name[..^7]}' constant type not supported.");
            }
        }

        public static LiteralExpression VisitStringLiteral(StringLiteralContext context)
        {
            return new LiteralExpression
            {
                Value = IdentifierUtility.Unescape(context.GetText()),
                ValueType = LiteralValueType.String
            };
        }

        public static MultipleExpression VisitArrayExpression(ArrayExpressionContext context)
        {
            return new MultipleExpression
            {
                Expressions = context.constant().Select(VisitConstant).Cast<IExpression>().ToArray()
            };
        }

        public static ArrayAppendExpression VisitArrayAddExpression(ArrayAddExpressionContext context)
        {
            return new ArrayAppendExpression
            {
                AppendItem = new MultipleExpression
                {
                    Expressions = context.constant().Select(VisitConstant).Cast<IExpression>().ToArray()
                }
            };
        }

        public static MultipleExpression VisitExpressions(ExpressionsContext context)
        {
            return new MultipleExpression
            {
                Expressions = context.expression().Select(VisitExpression).ToArray()
            };
        }

        public static MemberExpression VisitFullColumnNameExpressionAtom(FullColumnNameExpressionAtomContext context)
        {
            return VisitFullColumnName(context.fullColumnName());
        }

        public static MemberExpression VisitFullColumnName(FullColumnNameContext context)
        {
            return new MemberExpression
            {
                Name = IdentifierUtility.Parse(context.GetText())
            };
        }

        public static FunctionExpression VisitFunctionCall(FunctionCallContext context)
        {
            if (context.builtInFunctionCall() != null)
                return VisitBuiltInFunctionCall(context.builtInFunctionCall());

            if (context.nativeFunctionCall() != null)
                return VisitNativeFunctionCall(context.nativeFunctionCall());

            return null;
        }

        public static FunctionExpression VisitBuiltInFunctionCall(BuiltInFunctionCallContext context)
        {
            // TODO: Implement
            throw new NotSupportedException("Not supported built-in functions yet.");
        }

        public static FunctionExpression VisitNativeFunctionCall(NativeFunctionCallContext context)
        {
            switch (context)
            {
                case UpdateItemFunctionCallContext updateItemFunctionCallContext:
                    return VisitUpdateItemFunction(updateItemFunctionCallContext.updateItemFunction());

                case ConditionExpressionFunctionCallContext conditionExpressionFunctionCallContext:
                    return VisitConditionExpressionFunction(conditionExpressionFunctionCallContext.conditionExpressionFunction());
            }

            return null;
        }

        // native function for update-item
        public static FunctionExpression VisitUpdateItemFunction(UpdateItemFunctionContext context)
        {
            switch (context)
            {
                case IfNotExistsFunctionCallContext ifNotExistsFunctionCallContext:
                    return new FunctionExpression
                    {
                        Member = VisitorHelper.GetMemberByName("IF_NOT_EXISTS"),
                        Parameters = new IExpression[]
                        {
                            VisitFullColumnName(ifNotExistsFunctionCallContext.fullColumnName()),
                            VisitConstant(ifNotExistsFunctionCallContext.constant())
                        }
                    };
            }

            return null;
        }

        public static FunctionExpression VisitConditionExpressionFunction(ConditionExpressionFunctionContext context)
        {
            switch (context)
            {
                case AttributeExistsFunctionCallContext attributeExistsFunctionCallContext:
                    return new FunctionExpression
                    {
                        Member = VisitorHelper.GetMemberByName("ATTRIBUTE_EXISTS"),
                        Parameters = new IExpression[]
                        {
                            VisitFullColumnName(attributeExistsFunctionCallContext.fullColumnName()),
                        }
                    };

                case AttributeNotExistsFunctionCallContext attributeNotExistsFunctionCallContext:
                    return new FunctionExpression
                    {
                        Member = VisitorHelper.GetMemberByName("ATTRIBUTE_NOT_EXISTS"),
                        Parameters = new IExpression[]
                        {
                            VisitFullColumnName(attributeNotExistsFunctionCallContext.fullColumnName()),
                        }
                    };

                case AttributeTypeFunctionCallContext attributeTypeFunctionCallContext:
                    return new FunctionExpression
                    {
                        Member = VisitorHelper.GetMemberByName("ATTRIBUTE_TYPE"),
                        Parameters = new IExpression[]
                        {
                            VisitFullColumnName(attributeTypeFunctionCallContext.fullColumnName()),
                            new LiteralExpression
                            {
                                Value = VisitorHelper.DataTypeToDynamoDBType(attributeTypeFunctionCallContext.dataType().GetText()),
                                ValueType = LiteralValueType.String
                            }
                        }
                    };

                case BeginsWithFunctionCallContext beginsWithFunctionCallContext:
                    return new FunctionExpression
                    {
                        Member = VisitorHelper.GetMemberByName("BEGINS_WITH"),
                        Parameters = new IExpression[]
                        {
                            VisitFullColumnName(beginsWithFunctionCallContext.fullColumnName()),
                            VisitStringLiteral(beginsWithFunctionCallContext.stringLiteral())
                        }
                    };

                case ContainsFunctionCallContext containsFunctionCallContext:
                    return new FunctionExpression
                    {
                        Member = VisitorHelper.GetMemberByName("CONTAINS"),
                        Parameters = new IExpression[]
                        {
                            VisitFullColumnName(containsFunctionCallContext.fullColumnName()),
                            VisitStringLiteral(containsFunctionCallContext.stringLiteral())
                        }
                    };

                case SizeFunctionCallContext sizeFunctionCallContext:
                    return new FunctionExpression
                    {
                        Member = VisitorHelper.GetMemberByName("SIZE"),
                        Parameters = new IExpression[]
                        {
                            VisitFullColumnName(sizeFunctionCallContext.fullColumnName())
                        }
                    };
            }

            return null;
        }

        public static IExpression VisitNestedExpressionAtom(NestedExpressionAtomContext context)
        {
            IExpression[] expressions = context.expression().Select(VisitExpression).ToArray();

            return new MultipleExpression
            {
                Expressions = expressions
            };
        }
    }
}
