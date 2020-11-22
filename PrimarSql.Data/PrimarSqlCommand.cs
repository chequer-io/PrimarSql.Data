using System;
using System.Data;
using System.Data.Common;
using Amazon.DynamoDBv2;
using PrimarSql.Data.Models;
using PrimarSql.Data.Visitors;

namespace PrimarSql.Data
{
    public sealed class PrimarSqlCommand : DbCommand
    {
        #region Properties
        public override string CommandText { get; set; }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType { get; set; }

        public override UpdateRowSource UpdatedRowSource
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        protected override DbConnection DbConnection { get; set; }

        protected override DbParameterCollection DbParameterCollection { get; }

        protected override DbTransaction DbTransaction { get; set; }

        public override bool DesignTimeVisible
        {
            get => false;
            set => throw new NotSupportedException();
        }

        private AmazonDynamoDBClient Client => ((PrimarSqlConnection)Connection).Client;
        #endregion

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
            var root = PrimarSqlParser.Parse(CommandText);
            var queryPlanner = ContextVisitor.Visit(root);
            queryPlanner.QueryContext = new QueryContext(Client);
            
            return queryPlanner.Execute();
        }

        public override void Cancel()
        {
        }
        #endregion
    }
}
