using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Models.Conditions
{
    public interface ICondition
    {
        bool IsActivated { get; }

        string ToExpression(AttributeValueManager valueManager);
    }
}
