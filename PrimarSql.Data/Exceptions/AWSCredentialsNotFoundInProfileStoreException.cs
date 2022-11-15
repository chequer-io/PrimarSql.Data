namespace PrimarSql.Data.Exceptions;

internal sealed class AWSCredentialsNotFoundInProfileStoreException : PrimarSqlException
{
    public AWSCredentialsNotFoundInProfileStoreException(string profileName)
        : base(PrimarSqlError.AWSCredentialsNotFoundInProfileStore, $"Failed to find '{profileName}' profile from profile store.")
    {
    }
}
