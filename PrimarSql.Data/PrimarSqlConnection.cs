using System;
using System.Data;
using System.Data.Common;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using PrimarSql.Data.Extensions;

namespace PrimarSql.Data
{
    public sealed class PrimarSqlConnection : DbConnection
    {
        #region Properties
        public override string Database => string.Empty;

        public override string DataSource => ConnectionStringBuilder.EndPoint;

        public override string ServerVersion => throw new NotSupportedException();

        public override string ConnectionString
        {
            get => ConnectionStringBuilder.ToString();
            set => ConnectionStringBuilder = new PrimarSqlConnectionStringBuilder(value);
        }

        private PrimarSqlConnectionStringBuilder ConnectionStringBuilder { get; set; } = new PrimarSqlConnectionStringBuilder();

        public override ConnectionState State => _state;

        internal AmazonDynamoDBClient Client { get; private set; }
        #endregion

        #region Fields
        private ConnectionState _state = ConnectionState.Closed;
        private bool _isDisposed;
        #endregion

        #region Constructor
        public PrimarSqlConnection()
        {
        }

        public PrimarSqlConnection(string connectionString) : this(new PrimarSqlConnectionStringBuilder(connectionString))
        {
        }

        public PrimarSqlConnection(PrimarSqlConnectionStringBuilder connectionStringBuilder) : this()
        {
            ConnectionStringBuilder = connectionStringBuilder;
        }
        #endregion

        #region Public Methods
        public override void Open()
        {
            try
            {
                _state = ConnectionState.Connecting;

                AWSCredentials credentials;

                if (!string.IsNullOrWhiteSpace(ConnectionStringBuilder.AccessKey) &&
                    !string.IsNullOrWhiteSpace(ConnectionStringBuilder.AccessSecretKey))
                {
                    credentials = new BasicAWSCredentials(ConnectionStringBuilder.AccessKey, ConnectionStringBuilder.AccessSecretKey);
                }
                else
                {
                    credentials = FallbackCredentialsFactory.GetCredentials();
                }

                if (ConnectionStringBuilder.IsStandalone)
                {
                    Client = new AmazonDynamoDBClient(credentials, new AmazonDynamoDBConfig
                    {
                        ServiceURL = ConnectionStringBuilder.EndPoint
                    });
                }
                else
                {
                    Client = new AmazonDynamoDBClient(credentials, ConnectionStringBuilder.AwsRegion.ToRegionEndpoint());
                }

                var _ = Client.DescribeEndpointsAsync(new DescribeEndpointsRequest()).Result;

                _state = ConnectionState.Open;
            }
            catch (Exception e)
            {
                while (e is AggregateException agg && agg.InnerExceptions.Count == 1)
                    e = agg.InnerExceptions[0];

                _state = ConnectionState.Broken;
                throw new PrimarSqlException(e.Message, e);
            }
        }

        public override void Close()
        {
            try
            {
                Client.Dispose();
                _state = ConnectionState.Closed;
            }
            catch
            {
                _state = ConnectionState.Broken;
            }
        }

        public DbCommand CreateDbCommand(string commandText)
        {
            var command = CreateDbCommand();
            command.CommandText = commandText;

            return command;
        }

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException();
        }

        protected override DbCommand CreateDbCommand()
        {
            VerifyConnectionState();

            return new PrimarSqlCommand()
            {
                Connection = this
            };
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException();
        }
        #endregion

        #region Private Methods
        private void VerifyConnectionState()
        {
            VerifyDispose();

            if (_state != ConnectionState.Open)
                throw new InvalidOperationException("Connection is not open.");
        }

        private void VerifyDispose()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(ToString());
        }
        #endregion

        #region IDisposable
        protected override void Dispose(bool disposing)
        {
            if (_isDisposed)
                return;

            Close();
            _isDisposed = true;

            base.Dispose(disposing);
        }
        #endregion

        #region ICloneable
        public object Clone()
        {
            VerifyConnectionState();
            return new PrimarSqlConnection(ConnectionStringBuilder);
        }
        #endregion
    }
}
