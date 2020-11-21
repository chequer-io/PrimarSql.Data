using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using PrimarSql.Data.Models;
using PrimarSql.Data.Planners;
using PrimarSql.Data.Visitors;

namespace PrimarSql.Data.Sample
{
    class Program
    {
        public const string ApiKey = "";
        public const string Secret = "";

        static void Main(string[] args)
        {
            var clientConfig = new AmazonDynamoDBConfig
            {
                RegionEndpoint = RegionEndpoint.APNortheast2 // Seoul
            };

            var credentials = new BasicAWSCredentials(ApiKey, Secret);

            var client = new AmazonDynamoDBClient(credentials, clientConfig);

            // string query = "DROP TABLE a, b, `c`";
            // string query = "SELECT STRONGLY * FROM actor.IncludeIndexTest WHERE a = 1";
            string query = "SELECT * FROM actor where actor_id = 2 OFFSET 0";
            // string query = "SELECT * FROM actor where 1 IN (actor_id, 2, 3)";

            // string query = "SELECT * FROM actor WHERE actor_id BETWEEN '1' AND '2'";
            
            var root = PrimarSqlParser.Parse(query);
            
            var planner = ContextVisitor.Visit(root);
            planner.QueryContext = new QueryContext(client);

            var reader = planner.Execute();
            reader.Read();
            var v = reader.GetValue(0);
        }
    }
}