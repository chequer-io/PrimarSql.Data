using System;

namespace PrimarSql.Data.Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            var root = PrimarSqlParser.Parse("DROP TABLE a.b, b, `c`");
            var processor = new ContextProcessor();
            processor.Process(root);
        }
    }
}