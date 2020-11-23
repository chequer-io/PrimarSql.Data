namespace PrimarSql.Data.Planners
{
    internal sealed class DropIndexQueryInfo : IQueryInfo
    {
        public string TableName { get; }

        public string TargetIndex { get; }

        public DropIndexQueryInfo(string tableName, string targetIndex)
        {
            TableName = tableName;
            TargetIndex = targetIndex;
        }
    }
}
