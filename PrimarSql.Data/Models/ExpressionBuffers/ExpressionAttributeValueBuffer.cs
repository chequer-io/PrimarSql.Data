namespace PrimarSql.Data.Models.ExpressionBuffers
{
    public class ExpressionAttributeValueBuffer : IBuffer
    {
        public ExpressionAttributeValue ExpressionAttributeValue { get; set; }

        public ExpressionAttributeValueBuffer(ExpressionAttributeValue attrValue)
        {
            ExpressionAttributeValue = attrValue;
        }
        
        public override string ToString()
        {
            return ExpressionAttributeValue?.ToString();
        }
    }
}
