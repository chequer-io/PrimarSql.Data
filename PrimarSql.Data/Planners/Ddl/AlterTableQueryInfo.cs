using System;
using System.Collections.Generic;
using PrimarSql.Data.Planners.Index;
using PrimarSql.Data.Planners.Table;

namespace PrimarSql.Data.Planners
{
    internal sealed class AlterTableQueryInfo : TableQueryInfo
    {
        private readonly Dictionary<string, TableColumn> _tableColumns = new Dictionary<string, TableColumn>();
        private readonly List<IndexAction> _indexActions = new List<IndexAction>();

        public IEnumerable<TableColumn> TableColumns => _tableColumns.Values;

        public IEnumerable<IndexAction> IndexActions => _indexActions.AsReadOnly();

        public void AddTableColumn(TableColumn tableColumn)
        {
            var name = tableColumn.ColumnName;

            if (_tableColumns.ContainsKey(name))
                throw new InvalidOperationException($"Already '{name}' is defined.");

            _tableColumns[name] = tableColumn;
        }

        public void AddIndexAddAction(IndexDefinition indexDefinition)
        {
            if (indexDefinition.IsLocalIndex)
                throw new InvalidOperationException("Cannot add local index. Local index can add when create table.");

            _indexActions.Add(new AddIndexAction
            {
                IndexDefinition = indexDefinition
            });
        }

        public void AddIndexDropAction(string indexName)
        {
            _indexActions.Add(new DropIndexAction
            {
                IndexName = indexName
            });
        }

        public void AddIndexAlterAction(string indexName, int readCapacity, int writeCapacity)
        {
            _indexActions.Add(new UpdateIndexAction
            {
                IndexName = indexName,
                ReadCapacity = readCapacity,
                WriteCapacity = writeCapacity
            });
        }
    }
}
