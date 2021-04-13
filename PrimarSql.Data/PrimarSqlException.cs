using System;
using System.Data.Common;
using System.Runtime.Serialization;

namespace PrimarSql.Data
{
    public sealed class PrimarSqlException : DbException
    {
        public PrimarSqlException()
        {
        }

        public PrimarSqlException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public PrimarSqlException(string message) : base(message)
        {
        }

        public PrimarSqlException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public PrimarSqlException(string message, int errorCode) : base(message, errorCode)
        {
        }
    }
}
