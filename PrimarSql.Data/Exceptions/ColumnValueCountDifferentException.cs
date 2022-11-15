namespace PrimarSql.Data.Exceptions;

internal sealed class ColumnValueCountDifferentException : PrimarSqlException
{
    public ColumnValueCountDifferentException()
        : base(PrimarSqlError.ColumnValueCountDifferent, "The number of values does not match the number of columns.")
    {
    }
}
