dotnet build PrimarSql.Data -c Release --no-incremental
dotnet pack PrimarSql.Data -c Release -p:Packaging=true -o build
