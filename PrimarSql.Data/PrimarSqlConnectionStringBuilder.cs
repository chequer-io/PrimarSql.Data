using System;
using System.Data.Common;
using PrimarSql.Data.Models;

namespace PrimarSql.Data
{
    public sealed class PrimarSqlConnectionStringBuilder : DbConnectionStringBuilder
    {
        #region Properties
        public string AccessKey
        {
            get => GetValue<string>(nameof(AccessKey));
            set => SetValue(nameof(AccessKey), value);
        }

        public string AccessSecretKey
        {
            get => GetValue<string>(nameof(AccessSecretKey));
            set => SetValue(nameof(AccessSecretKey), value);
        }

        public AwsRegion AwsRegion
        {
            get => (AwsRegion)GetValue<int>(nameof(AwsRegion));
            set => SetValue(nameof(AwsRegion), (int)value);
        }
        
        public bool IsStandalone
        {
            get => GetValue<bool>(nameof(IsStandalone));
            set => SetValue(nameof(IsStandalone), value);
        }

        public string EndPoint
        {
            get => GetValue<string>(nameof(EndPoint));
            set => SetValue(nameof(EndPoint), value);
        }
        #endregion

        #region Constructor
        public PrimarSqlConnectionStringBuilder()
        {
        }

        public PrimarSqlConnectionStringBuilder(string connectionString) : this()
        {
            ConnectionString = connectionString;
        }
        #endregion

        #region Private Methods
        private T GetValue<T>(string key)
        {
            if (TryGetValue(key, out var value))
                return (T)Convert.ChangeType(value, typeof(T));

            return default;
        }

        private void SetValue<T>(string key, T value)
        {
            if (!string.IsNullOrEmpty(value?.ToString()))
            {
                this[key] = value;
            }
            else
            {
                Remove(key);
            }
        }
        #endregion
    }
}
