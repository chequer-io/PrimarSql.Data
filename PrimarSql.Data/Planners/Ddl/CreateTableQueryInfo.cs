using System;
using System.Collections.Generic;
using PrimarSql.Data.Planners.Index;
using PrimarSql.Data.Planners.Table;

namespace PrimarSql.Data.Planners
{
    internal sealed class CreateTableQueryInfo : TableQueryInfo
    {
        private readonly Dictionary<string, KeyTableColumn> _tableColumns = new Dictionary<string, KeyTableColumn>();
        private readonly Dictionary<string, IndexDefinition> _indexDefinitions = new Dictionary<string, IndexDefinition>();
        private string _hashKeyName = string.Empty;
        private string _sortKeyName = string.Empty;
        private int _tempItemCount = 0;
        
        public bool SkipIfExists { get; set; }

        public IEnumerable<KeyTableColumn> TableColumns => _tableColumns.Values;

        public IEnumerable<IndexDefinition> IndexDefinitions => _indexDefinitions.Values;

        public KeyTableColumn HashKeyColumn => _tableColumns.TryGetValue(_hashKeyName, out var value) ? value : null;

        public KeyTableColumn SortKeyColumn => _tableColumns.TryGetValue(_sortKeyName, out var value) ? value : null;

        public void AddTableColumn(KeyTableColumn keyTableColumn)
        {
            var name = keyTableColumn.ColumnName;

            if (_tableColumns.TryGetValue(name, out var definedColumn))
            {
                if (string.IsNullOrEmpty(definedColumn.DataType))
                {
                    if (keyTableColumn.IsHashKey || keyTableColumn.IsSortKey)
                        throw new PrimarSqlException(PrimarSqlError.Syntax, $"Key '{name}' constraint definition is duplicated.");

                    keyTableColumn.IsHashKey = definedColumn.IsHashKey;
                    keyTableColumn.IsSortKey = definedColumn.IsSortKey;
                    
                    _tableColumns[name] = keyTableColumn;
                    _tempItemCount--;
                    return;
                }

                throw new PrimarSqlException(PrimarSqlError.Syntax, $"Already '{name}' is defined.");
            }

            if (keyTableColumn.IsHashKey)
            {
                if (HashKeyColumn != null)
                    throw new PrimarSqlException(PrimarSqlError.Syntax, $"Hash key duplicated. (name: '{_hashKeyName}', '{name}')");

                _hashKeyName = name;
            }
            else if (keyTableColumn.IsSortKey)
            {
                if (SortKeyColumn != null)
                    throw new PrimarSqlException(PrimarSqlError.Syntax, $"Sort key duplicated. (sort key: '{_sortKeyName}', '{name}')");

                _sortKeyName = name;
            }

            _tableColumns[name] = keyTableColumn;
        }

        public void SetConstraint(string tableName, bool isHashKey)
        {
            if (_tableColumns.ContainsKey(tableName))
            {
                var column = _tableColumns[tableName];   
                if (column.IsHashKey || column.IsSortKey)
                    throw new PrimarSqlException(PrimarSqlError.Syntax, $"Key '{tableName}' constraint definition is duplicated.");
                
                column.IsHashKey = isHashKey;
                column.IsSortKey = !isHashKey;
            }
            else
            {
                _tableColumns[tableName] = new KeyTableColumn
                {
                    ColumnName = tableName,
                    IsHashKey = isHashKey,
                    IsSortKey = !isHashKey
                };

                _tempItemCount++;
            }
            
            if (isHashKey)
                _hashKeyName = tableName;
            else
                _sortKeyName = tableName;
        }

        public void AddIndexDefinition(IndexDefinition definition)
        {
            if (_indexDefinitions.ContainsKey(definition.IndexName))
                throw new PrimarSqlException(PrimarSqlError.Syntax, $"Index name '{definition.IndexName}' duplicate.");

            _indexDefinitions[definition.IndexName] = definition;
        }
        
        public void Validate()
        {
            if (_tempItemCount != 0)
                throw new PrimarSqlException(PrimarSqlError.Syntax, "Constraint definition column name must exists.");
            
            if (HashKeyColumn == null)
                throw new PrimarSqlException(PrimarSqlError.Syntax, $"No hash key defined for Table '{TableName}'.");
        }
    }
}
