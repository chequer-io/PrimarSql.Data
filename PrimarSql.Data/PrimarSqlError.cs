namespace PrimarSql.Data
{
    public enum PrimarSqlError
    {
        Unknown,

        // Internal
        Syntax = 1,
        AWSCredentialsNotFoundInProfileStore,
        ColumnValueCountDifferent,
        ColumnValueCountExceed,
        NotSupportedFeature,

        // AmazonDynamoDBException derived classes
        BackupInUse = 1000,
        BackupNotFound,
        ConditionalCheckFailed,
        ContinuousBackupsUnavailable,
        DuplicateItem,
        ExpiredIterator,
        ExportConflict,
        ExportNotFound,
        GlobalTableAlreadyExists,
        GlobalTableNotFound,
        IdempotentParameterMismatch,
        ImportConflict,
        ImportNotFound,
        IndexNotFound,
        InternalServerError,
        InvalidExportTime,
        InvalidRestoreTime,
        ItemCollectionSizeLimitExceeded,
        LimitExceeded,
        PointInTimeRecoveryUnavailable,
        ProvisionedThroughputExceeded,
        ReplicaAlreadyExists,
        ReplicaNotFound,
        RequestLimitExceeded,
        ResourceInUse,
        ResourceNotFound,
        TableAlreadyExists,
        TableInUse,
        TableNotFound,
        TransactionCanceled,
        TransactionConflict,
        TransactionInProgress,
        TrimmedDataAccess
    }
}
