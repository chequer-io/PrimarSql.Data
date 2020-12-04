using System;
using System.Collections.Generic;
using PrimarSql.Data.Models;
using Spectre.Console;

namespace PrimarSql.Data.Sample
{
    class Program
    {
        public const string ApiKey = "";
        public const string Secret = "";

        static void Main(string[] args)
        {
            var apiKey = ApiKey;
            var secret = Secret;

            if (string.IsNullOrEmpty(apiKey))
                apiKey = AnsiConsole.Ask<string>("Enter your DynamoDB [blue]API Key[/]");

            TextPrompt<string> secretPrompt = new TextPrompt<string>("Enter your DynamoDB [blue]API Secret[/]")
                .PromptStyle("red")
                .Secret();

            if (string.IsNullOrEmpty(secret))
                secret = AnsiConsole.Prompt(secretPrompt);

            TextPrompt<string> regionPrompt = new TextPrompt<string>("Enter your DynamoDB [blue]Region[/]?")
                .InvalidChoiceMessage("[red]Invalid Region[/]")
                .DefaultValue("APNortheast2");

            foreach (string regionName in Enum.GetNames(typeof(AwsRegion)))
                regionPrompt.AddChoice(regionName);

            var region = AnsiConsole.Prompt(regionPrompt);

            var connection = new PrimarSqlConnection(new PrimarSqlConnectionStringBuilder
            {
                AccessKey = apiKey,
                AccessSecretKey = secret,
                AwsRegion = Enum.Parse<AwsRegion>(region),
            });

            connection.Open();

            while (true)
            {
                try
                {
                    var query = AnsiConsole.Prompt(new TextPrompt<string>("[blue]Query[/]"));
                    var command = connection.CreateDbCommand(query);
                    using var dbDataReader = command.ExecuteReader();

                    var table = new Table
                    {
                        Border = TableBorder.Rounded
                    };

                    for (int i = 0; i < dbDataReader.FieldCount; i++)
                        table.AddColumn($"[green]{Markup.Escape(dbDataReader.GetName(i))}[/]");

                    while (dbDataReader.Read())
                    {
                        var list = new List<string>();

                        for (int i = 0; i < dbDataReader.FieldCount; i++)
                        {
                            var value = dbDataReader[i];

                            list.Add(Markup.Escape(value switch
                            {
                                byte[] bArr => Convert.ToBase64String(bArr),
                                DateTime dt => dt.ToString("u"),
                                _ => value.ToString()
                            }));
                        }

                        table.AddRow(list.ToArray());
                    }

                    AnsiConsole.Render(table);
                    AnsiConsole.MarkupLine($"RecordsAffected: [green]{dbDataReader.RecordsAffected}[/]");
                }
                catch (AggregateException e)
                {
                    AnsiConsole.Markup("[bold white on red]{0}[/]", Markup.Escape(e.InnerExceptions[0].Message));
                    Console.WriteLine();
                }
                catch (Exception e)
                {
                    AnsiConsole.Markup("[bold white on red]{0}[/]", Markup.Escape(e.Message));
                    Console.WriteLine();
                }
            }
        }
    }
}
