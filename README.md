<img width="128" src="https://github.com/chequer-io/primarsql.data/blob/main/Logo.png?raw=true">

# PrimarSql.Data [![Nuget](https://img.shields.io/nuget/v/PrimarSql.Data)](https://www.nuget.org/packages/PrimarSql.Data/)

## Overview

PrimarSql (SQL for DynamoDB) ADO.NET Provider

## Example

``` csharp
string apiKey = "<Your DynamoDB Api Key>";
string secret = "<Your DynamoDB Api Secret>";

var connection = new PrimarSqlConnection(new PrimarSqlConnectionStringBuilder
{
    AccessKey = apiKey,
    AccessSecretKey = secret,
    AwsRegion = AwsRegion.APNortheast2
});

connection.Open();

string query = "SELECT * FROM table";

var command = connection.CreateDbCommand(query);
var dbDataReader = command.ExecuteReader();

while (dbDataReader.Read())
{
    // ...
}

```
