namespace PrimarSql.Data.Models
{
    public class SortKey : IKey
    {
        public ExpressionAttributeName ExpressionAttributeName { get; set; }

        public ExpressionAttributeValue ExpressionAttributeValue { get; set; }

        // when BETWEEN
        public ExpressionAttributeValue ExpressionAttributeValue2 { get; set; }

        public string Operator { get; set; }

        public SortKeyType SortKeyType { get; set; }

        public override string ToString()
        {
            switch (SortKeyType)
            {
                case SortKeyType.Comparison:
                    return $"{ExpressionAttributeName.Key} {Operator} {ExpressionAttributeValue.Key}";

                case SortKeyType.Between:
                    return $"{ExpressionAttributeName.Key} BETWEEN {ExpressionAttributeValue.Key} AND {ExpressionAttributeValue2.Key}";

                case SortKeyType.BeginsWith:
                    return $"begins_with({ExpressionAttributeName.Key}, {ExpressionAttributeValue.Key})";
            }

            return string.Empty;
        }
    }
}
