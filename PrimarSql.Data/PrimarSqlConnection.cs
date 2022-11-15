using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using PrimarSql.Data.Core;
using PrimarSql.Data.Exceptions;
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

        internal IAmazonDynamoDB Client { get; private set; }
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

                var credentials = CreateCredentials();

                if (ConnectionStringBuilder.IsStandalone)
                {
                    Client = new AmazonDynamoDBWrapper(
                        new AmazonDynamoDBClient(
                            credentials,
                            new AmazonDynamoDBConfig
                            {
                                ServiceURL = ConnectionStringBuilder.EndPoint
                            }
                        )
                    );
                }
                else
                {
                    Client = new AmazonDynamoDBWrapper(
                        new AmazonDynamoDBClient(
                            credentials,
                            ConnectionStringBuilder.AwsRegion.ToRegionEndpoint()
                        )
                    );
                }

                var _ = Client.DescribeEndpointsAsync(new DescribeEndpointsRequest()).GetResultSynchronously();

                _state = ConnectionState.Open;
            }
            catch (Exception e)
            {
                _state = ConnectionState.Broken;

                if (e is PrimarSqlException)
                    throw;

                throw new PrimarSqlException(PrimarSqlError.Unknown, e);
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

        private AWSCredentials CreateCredentials()
        {
            var builder = ConnectionStringBuilder;

            if (!string.IsNullOrWhiteSpace(builder.AccessKey) &&
                !string.IsNullOrWhiteSpace(builder.AccessSecretKey))
            {
                // IAM Account
                return new BasicAWSCredentials(builder.AccessKey, builder.AccessSecretKey);
            }

            if (!string.IsNullOrWhiteSpace(builder.CredentialsFilePath) &&
                !string.IsNullOrWhiteSpace(builder.ProfileName))
            {
                // Credentials from shared credential file
                if (!File.Exists(builder.CredentialsFilePath))
                    throw new FileNotFoundException($"File '{builder.CredentialsFilePath}' not exists.");

                var sharedFile = new SharedCredentialsFile(builder.CredentialsFilePath);

                if (!sharedFile.TryGetProfile(builder.ProfileName, out var profile))
                    throw new KeyNotFoundException($"Profile name '{builder.ProfileName}' not found.");

                return profile.GetAWSCredentials(sharedFile);
            }

            if (!string.IsNullOrWhiteSpace(builder.ProfileName))
            {
                // Credentials from config
                var chain = new CredentialProfileStoreChain();

                if (!chain.TryGetAWSCredentials(builder.ProfileName, out var credentials))
                    throw new AWSCredentialsNotFoundInProfileStoreException(builder.ProfileName);

                if (credentials is SSOAWSCredentials ssoCredentials)
                {
                    ssoCredentials.Options.ClientName = string.IsNullOrWhiteSpace(builder.ClientName)
                        ? "PrimarSql.Data"
                        : builder.ClientName;

                    ssoCredentials.Options.SsoVerificationCallback = args =>
                    {
                        Process.Start(new ProcessStartInfo
                        {
                            FileName = args.VerificationUriComplete,
                            UseShellExecute = true
                        });
                    };
                }

                return credentials;
            }

            return FallbackCredentialsFactory.GetCredentials();
        }

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
