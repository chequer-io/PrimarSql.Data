using System;

namespace PrimarSql.Data.Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            var root = PrimarSqlParser.Parse("SELECT * FROM actor");
        }
    }
}