using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data.Planners.Index
{
    internal sealed class UpdateIndexAction : IndexAction
    {
        public string IndexName { get; set; }

        public int ReadCapacity { get; set; }

        public int WriteCapacity { get; set; }

        public override void Action(List<GlobalSecondaryIndexUpdate> indexUpdates, TableDescription tableDescription)
        {
            indexUpdates.Add(new GlobalSecondaryIndexUpdate
            {
                Update = new UpdateGlobalSecondaryIndexAction
                {
                    IndexName = IndexName,
                    ProvisionedThroughput = new ProvisionedThroughput(ReadCapacity, WriteCapacity)
                }
            });
        }
    }
}
