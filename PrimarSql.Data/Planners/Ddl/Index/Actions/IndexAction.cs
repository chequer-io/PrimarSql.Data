using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data.Planners.Index
{
    internal abstract class IndexAction
    {
        public abstract void Action(UpdateTableRequest request, TableDescription tableDescription);
    }
}
