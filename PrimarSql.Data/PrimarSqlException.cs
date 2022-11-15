using System;
using System.Data.Common;
using System.Text;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace PrimarSql.Data
{
    public class PrimarSqlException : DbException
    {
        public PrimarSqlError Error { get; }

        public override int ErrorCode => (int)Error;

        public PrimarSqlException(PrimarSqlError error, Exception e)
            : base(CreateMessage(error, null, e.Message), e)
        {
            Error = error;
        }

        public PrimarSqlException(PrimarSqlError error, string message)
            : base(CreateMessage(error, null, message))
        {
            Error = error;
        }

        private PrimarSqlException(PrimarSqlError error, string errorCode, Exception e)
            : base(CreateMessage(error, errorCode, e.Message))
        {
            Error = error;
        }

        private static string CreateMessage(PrimarSqlError error, string errorCode, string message)
        {
            var builder = new StringBuilder();

            // Error code
            builder.Append($"PS-{(int)error:0000}");

            if (!string.IsNullOrWhiteSpace(errorCode))
                builder.Append($"({errorCode})");

            if (!string.IsNullOrWhiteSpace(message))
                builder.Append(": ").Append(message);

            return builder.ToString();
        }

        public static PrimarSqlException From(AmazonServiceException e)
        {
            var (error, errorCode) = GetError(e);
            return new PrimarSqlException(error, errorCode, e);
        }

        private static (PrimarSqlError, string) GetError(AmazonServiceException e)
        {
            var error = e switch
            {
                BackupInUseException => PrimarSqlError.BackupInUse,
                BackupNotFoundException => PrimarSqlError.BackupNotFound,
                ConditionalCheckFailedException => PrimarSqlError.ConditionalCheckFailed,
                ContinuousBackupsUnavailableException => PrimarSqlError.ContinuousBackupsUnavailable,
                DuplicateItemException => PrimarSqlError.DuplicateItem,
                ExpiredIteratorException => PrimarSqlError.ExpiredIterator,
                ExportConflictException => PrimarSqlError.ExportConflict,
                ExportNotFoundException => PrimarSqlError.ExportNotFound,
                GlobalTableAlreadyExistsException => PrimarSqlError.GlobalTableAlreadyExists,
                GlobalTableNotFoundException => PrimarSqlError.GlobalTableNotFound,
                IdempotentParameterMismatchException => PrimarSqlError.IdempotentParameterMismatch,
                ImportConflictException => PrimarSqlError.ImportConflict,
                ImportNotFoundException => PrimarSqlError.ImportNotFound,
                IndexNotFoundException => PrimarSqlError.IndexNotFound,
                InternalServerErrorException => PrimarSqlError.InternalServerError,
                InvalidExportTimeException => PrimarSqlError.InvalidExportTime,
                InvalidRestoreTimeException => PrimarSqlError.InvalidRestoreTime,
                ItemCollectionSizeLimitExceededException => PrimarSqlError.ItemCollectionSizeLimitExceeded,
                LimitExceededException => PrimarSqlError.LimitExceeded,
                PointInTimeRecoveryUnavailableException => PrimarSqlError.PointInTimeRecoveryUnavailable,
                ProvisionedThroughputExceededException => PrimarSqlError.ProvisionedThroughputExceeded,
                ReplicaAlreadyExistsException => PrimarSqlError.ReplicaAlreadyExists,
                ReplicaNotFoundException => PrimarSqlError.ReplicaNotFound,
                RequestLimitExceededException => PrimarSqlError.RequestLimitExceeded,
                ResourceInUseException => PrimarSqlError.ResourceInUse,
                ResourceNotFoundException => PrimarSqlError.ResourceNotFound,
                TableAlreadyExistsException => PrimarSqlError.TableAlreadyExists,
                TableInUseException => PrimarSqlError.TableInUse,
                TableNotFoundException => PrimarSqlError.TableNotFound,
                TransactionCanceledException => PrimarSqlError.TransactionCanceled,
                TransactionConflictException => PrimarSqlError.TransactionConflict,
                TransactionInProgressException => PrimarSqlError.TransactionInProgress,
                TrimmedDataAccessException => PrimarSqlError.TrimmedDataAccess,
                _ => PrimarSqlError.Unknown
            };

            var codeName = e.ErrorCode;

            if (codeName?.EndsWith("Exception") is true)
                codeName = codeName[..^9];

            return (error, codeName);
        }
    }
}
