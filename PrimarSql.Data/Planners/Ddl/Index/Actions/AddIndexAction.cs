using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data.Planners.Index
{
    internal sealed class AddIndexAction : IndexAction
    {
        public IndexDefinition IndexDefinition { get; set; }

        public override void Action(List<GlobalSecondaryIndexUpdate> indexUpdates, TableDescription tableDescription)
        {
            if (IndexDefinition.IsLocalIndex)
                throw new PrimarSqlException(PrimarSqlError.Syntax, "Cannot add local index. Local index can add when create table.");

            var provisionedThroughput = new ProvisionedThroughput(
                tableDescription.ProvisionedThroughput.ReadCapacityUnits,
                tableDescription.ProvisionedThroughput.WriteCapacityUnits
            );
            
            indexUpdates.Add(new GlobalSecondaryIndexUpdate
            {
                Create = new CreateGlobalSecondaryIndexAction
                {
                    IndexName = IndexDefinition.IndexName,
                    Projection = IndexDefinition.Projection,
                    KeySchema = IndexDefinition.KeySchema,
                    ProvisionedThroughput = provisionedThroughput,
                }
            });
        }
    }
}
