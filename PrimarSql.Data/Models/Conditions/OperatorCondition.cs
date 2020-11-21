using System;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Models.Conditions
{
    public class OperatorCondition : ICondition
    {
        public ICondition Left { get; }

        public string Operator { get; }

        public ICondition Right { get; }

        public bool IsActivated => (Left?.IsActivated ?? false) && (Right?.IsActivated ?? false);

        public OperatorCondition(ICondition left, string @operator, ICondition right)
        {
            Left = left;
            Operator = ConvertOperator(@operator);
            Right = right;
        }

        public string ToExpression(AttributeValueManager valueManager)
        {
            var leftActivated = Left?.IsActivated ?? false;
            var rightActivated = Right?.IsActivated ?? false;

            if (IsAnd)
            {
                if (!leftActivated && !rightActivated)
                    return null;

                if (leftActivated && !rightActivated)
                    return Left.ToExpression(valueManager);

                if (!leftActivated && rightActivated)
                    return Right.ToExpression(valueManager);
            }

            if (IsOr)
            {
                if (!leftActivated && !rightActivated)
                    return null;

                if (leftActivated && !rightActivated)
                    return $"{Left.ToExpression(valueManager)} OR {valueManager.GetTrueLiteral()}";

                if (!leftActivated && rightActivated)
                    return $"{valueManager.GetTrueLiteral()} OR {Right.ToExpression(valueManager)}";
            }

            return $"{Left?.ToExpression(valueManager)} {Operator} {Right?.ToExpression(valueManager)}";
        }

        private bool IsAnd => Operator.EqualsIgnore("AND") || Operator.Equals("&&");

        private bool IsOr => Operator.EqualsIgnore("OR") || Operator.Equals("||");

        private string ConvertOperator(string @operator)
        {
            return @operator switch
            {
                "&&" => "AND",
                "||" => "OR",
                _ => @operator
            };
        }
    }
}
