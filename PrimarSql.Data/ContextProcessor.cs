using System;
using System.Linq;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Cursor;
using PrimarSql.Data.Planners;
using PrimarSql.Data.Utilities;
using static PrimarSql.Internal.PrimarSqlParser;
using static PrimarSql.Data.Utilities.Validator;

namespace PrimarSql.Data
{
    // TODO: internal
    public class ContextProcessor
    {
        private readonly QueryPlanner _queryPlanner;

        public QueryPlanner QueryPlanner => _queryPlanner;

        public ContextProcessor()
        {
        }

        public void Test()
        {
            AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
            clientConfig.RegionEndpoint = RegionEndpoint.USEast1;
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(clientConfig);

            var response = client.DeleteTableAsync(new DeleteTableRequest
            {
                TableName = "city"
            }).Result;
        }
        
        public ICursor Process(RootContext context)
        {
            var statement = context
                .sqlStatements()
                .sqlStatement(0);

            if (statement.children.Count == 0)
                return null;

            switch (statement.children[0])
            {
                case DdlStatementContext ddlStatementContext:
                    return ProcessDdlStatementContext(ddlStatementContext);
            }

            return null;
        }

        public ICursor ProcessDdlStatementContext(DdlStatementContext context)
        {
            if (context.children.Count == 0)
                return null;

            switch (context.children[0])
            {
                case CreateIndexContext createIndexContext:

                    break;

                case CreateTableContext createTableContext:

                    break;

                case AlterTableContext alterTableContext:

                    break;

                case DropIndexContext dropIndexContext:

                    break;

                case DropTableContext dropTableContext:
                    ProcessDropTableContext(dropTableContext);
                    break;
            }

            return null;
        }

        public void ProcessDropTableContext(DropTableContext context)
        {
            foreach (var tableName in context.tableName())
            {
                string[] result = IdentifierUtility.Parse(tableName.GetText());
                ValidateTableName(result);
                
                _queryPlanner.Tables.Add(result.FirstOrDefault());
            }
        }
    }
}
