namespace PrimarSql.Data.Planners.Show
{
    public class ShowIndexesQueryInfo : IQueryInfo
    {
        public string TableName { get; set; }
        
        public ShowIndexesQueryInfo(string tableName)
        {
            TableName = tableName;
        }
    }
}
