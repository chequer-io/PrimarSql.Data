namespace PrimarSql.Data.Models.ExpressionBuffers
{
    public class ExpressionAttributeNameBuffer: IBuffer
    {
        public ExpressionAttributeName ExpressionAttributeName { get; set; }

        public ExpressionAttributeNameBuffer(ExpressionAttributeName attrName)
        {
            ExpressionAttributeName = attrName;
        }

        public override string ToString()
        {
            return ExpressionAttributeName?.ToString();
        }
    }
}
