using System;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using PrimarSql.Data.Models;
using PrimarSql.Data.Processors;

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
            string query = "SELECT STRONGLY * FROM (SELECT * FROM actor)";
            
            var root = PrimarSqlParser.Parse(query);
            var processor = new ContextProcessor();
            var planner = processor.Process(root);
            planner.QueryContext = new QueryContext(client);


            var reader = planner.Execute();
            reader.Read();
            var v = reader.GetValue(0);
        }
    }
}