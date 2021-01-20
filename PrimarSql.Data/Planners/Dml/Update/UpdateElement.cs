using PrimarSql.Data.Expressions;
using PrimarSql.Data.Models.Columns;

namespace PrimarSql.Data.Planners
{
    internal struct UpdateElement
    {
        public bool IsRemove => Value == null;
        
        public IPart[] Name { get; set; }

        public IExpression Value { get; set; }
    }
}
