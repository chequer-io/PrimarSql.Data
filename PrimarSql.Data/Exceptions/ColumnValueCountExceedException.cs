namespace PrimarSql.Data.Exceptions;

public sealed class ColumnValueCountExceedException : PrimarSqlException
{
    public ColumnValueCountExceedException()
        : base(PrimarSqlError.ColumnValueCountExceed, "The number of values cannot exceed the number of columns.")
    {
    }
}
