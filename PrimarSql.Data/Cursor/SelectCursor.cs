using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Cursor.Providers;
using PrimarSql.Data.Expressions.Generators;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models;
using PrimarSql.Data.Sources;

namespace PrimarSql.Data.Cursor
{
    // TODO: Expression
    public class SelectCursor : DataCursor
    {
        private IDataProvider DataProvider { get; set; }
        
        public QueryContext Context { get; }

        public SelectQueryInfo QueryInfo { get; }

        public override bool IsClosed => false;

        public override bool HasRows => DataProvider.HasRows;

        public override int RecordsAffected => -1;

        public SelectCursor(QueryContext context, SelectQueryInfo queryInfo)
        {
            Context = context;
            QueryInfo = queryInfo;

            DataProvider = QueryInfo.TableSource switch
            {
                AtomTableSource _ => new ApiDataProvider(Context, QueryInfo),
                SubquerySource _ => new SubqueryDataProvider(Context, QueryInfo),
                _ => DataProvider
            };
        }

        public override DataTable GetSchemaTable()
        {
            return DataProvider.GetSchemaTable();
        }

        public override bool Read()
        {
            if (!DataProvider.Next())
                return false;
            
            CurrentRow = DataProvider.Current;
            return true;
        }

    }
}
