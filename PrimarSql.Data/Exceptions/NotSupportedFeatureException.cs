namespace PrimarSql.Data.Exceptions;

internal sealed class NotSupportedFeatureException : PrimarSqlException
{
    public NotSupportedFeatureException(string message)
        : base(PrimarSqlError.NotSupportedFeature, message)
    {
    }
}
