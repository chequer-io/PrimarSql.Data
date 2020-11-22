using System;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Sample
{
    class Program
    {
        public const string ApiKey = "";
        public const string Secret = "";

        static void Main(string[] args)
        {
            // string query = "DROP TABLE a, b, `c`";
            // string query = "SELECT STRONGLY * FROM actor.IncludeIndexTest WHERE a = 1";
            // string query = "SELECT * FROM city where country_id = 7 AND city_id = 586 AND city = 'Yeravan'";
            // string query = "SELECT * FROM actor where actor_id = 7 AND last_name = 'MOSTEL'";
            // string query = "SELECT * FROM actor where 1 IN (actor_id, 2, 3)";
            // string query = "SELECT `a.b`, test[0].a, test[0], test[1] FROM test WHERE `a.b` = 'aaaaaaa'";
            // string query = "SELECT * FROM actor WHERE actor_id BETWEEN '1' AND '2'";
            
            var connection = new PrimarSqlConnection(new PrimarSqlConnectionStringBuilder
            {
                AccessKey = ApiKey,
                AccessSecretKey = Secret,
                AwsRegion = AwsRegion.APNortheast2,
            });

            connection.Open();

            var command = connection.CreateDbCommand("SELECT last_name, actor_id, last_update, first_name FROM actor");

            var dbDataReader = command.ExecuteReader();
            
            while (dbDataReader.Read())
            {
                Console.Write("|");

                for (int i = 0; i < dbDataReader.FieldCount; i++)
                {
                    Console.Write(dbDataReader[i]);
                    Console.Write("|");
                }

                Console.ReadLine();
            }

            Console.WriteLine("==End==");
        }
    }
}