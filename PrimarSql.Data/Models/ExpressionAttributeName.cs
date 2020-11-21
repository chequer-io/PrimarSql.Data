namespace PrimarSql.Data.Models
{
    public class ExpressionAttributeName
    {
        public string Key { get; }
        
        public string Value { get; }
        
        public ExpressionAttributeName(string key, string value)
        {
            Key = key;
            Value = value;
        }

        public override string ToString()
        {
            return Key;
        }
    }
}
