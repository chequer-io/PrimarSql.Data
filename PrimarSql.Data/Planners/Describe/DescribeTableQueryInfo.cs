namespace PrimarSql.Data.Planners.Describe
{
    internal sealed class DescribeTableQueryInfo : IQueryInfo
    {
        public string TableName { get; set; }
        
        public DescribeTableQueryInfo(string tableName)
        {
            TableName = tableName;
        }
    }
}
