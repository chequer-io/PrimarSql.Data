using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data.Planners.Index
{
    internal sealed class DropIndexAction : IndexAction
    {
        public string IndexName { get; set; }
        
        public override void Action(UpdateTableRequest request)
        {
            request.GlobalSecondaryIndexUpdates.Add(new GlobalSecondaryIndexUpdate
            {
                Delete = new DeleteGlobalSecondaryIndexAction
                {
                    IndexName = IndexName
                }
            });
        }
    }
}
