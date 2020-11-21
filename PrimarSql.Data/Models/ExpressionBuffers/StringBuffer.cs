namespace PrimarSql.Data.Models.ExpressionBuffers
{
    public class StringBuffer : IBuffer
    {
        public string Value { get; set; }
        
        public StringBuffer(string value)
        {
            Value = value;
        }

        public override string ToString()
        {
            return Value;
        }
    }
}
