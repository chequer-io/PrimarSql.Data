namespace PrimarSql.Data.Models
{
    public class HashKey : IKey
    {
        public ExpressionAttributeName ExpressionAttributeName { get; set; }
        
        public ExpressionAttributeValue ExpressionAttributeValue { get; set; }

        public int StartToken { get; set; }

        public int EndToken { get; set; }
        
        public override string ToString()
        {
            return $"{ExpressionAttributeName.Key} = {ExpressionAttributeValue.Key}";
        }
    }
}
