using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models;
using PrimarSql.Data.Visitors;

namespace PrimarSql.Data
{
    public sealed class PrimarSqlCommand : DbCommand
    {
        #region Properties
        public override string CommandText { get; set; }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType
        {
            get => CommandType.Text;
            set
            {
                if (value == CommandType.Text)
                    return;

                throw new NotSupportedException(value.ToString());
            }
        }

        public IList<IDocumentFilter> DocumentFilters { get; } = new List<IDocumentFilter>();

        public override UpdateRowSource UpdatedRowSource
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        protected override DbConnection DbConnection { get; set; }

        protected override DbParameterCollection DbParameterCollection { get; }

        protected override DbTransaction DbTransaction { get; set; }

        public bool IsCanceled => CancellationTokenSource.IsCancellationRequested;

        public override bool DesignTimeVisible
        {
            get => false;
            set => throw new NotSupportedException();
        }

        private IAmazonDynamoDB Client => ((PrimarSqlConnection)Connection).Client;
        #endregion

        internal CancellationTokenSource CancellationTokenSource { get; private set; }

        #region Public Methods
        public override void Prepare()
        {
            throw new NotSupportedException();
        }

        public override int ExecuteNonQuery()
        {
            using var dbDataReader = ExecuteDbDataReader(CommandBehavior.Default);

            return dbDataReader?.RecordsAffected ?? 0;
        }

        public override object ExecuteScalar()
        {
            object result = null;

            using var dbDataReader = ExecuteDbDataReader(CommandBehavior.Default);

            if (dbDataReader == null)
                return null;

            if (dbDataReader.Read() && dbDataReader.FieldCount > 0)
                result = dbDataReader.GetValue(0);

            return result;
        }

        protected override DbParameter CreateDbParameter()
        {
            throw new NotSupportedException();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetResultSynchronously();
        }

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            var root = Parse(CommandText);
            var queryPlanner = ContextVisitor.Visit(root);

            queryPlanner.Context = new QueryContext(Client, this)
            {
                DocumentFilters = DocumentFilters
            };

            CancellationTokenSource = new CancellationTokenSource();
            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, CancellationTokenSource.Token);

            return queryPlanner.ExecuteAsync(linkedCts.Token);
        }

        private Internal.PrimarSqlParser.RootContext Parse(string commandText)
        {
            try
            {
                return PrimarSqlParser.Parse(commandText);
            }
            catch (PrimarSqlSyntaxException e)
            {
                throw new PrimarSqlException(PrimarSqlError.Syntax, e);
            }
        }

        public override void Cancel()
        {
            CancellationTokenSource?.Cancel();
        }
        #endregion
    }
}
