using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Models.Conditions
{
    internal interface ICondition
    {
        bool IsActivated { get; }

        string ToExpression(AttributeValueManager valueManager);
    }
}
