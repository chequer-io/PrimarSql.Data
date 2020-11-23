using System;
using System.Collections.Generic;
using System.Linq;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Planners;
using PrimarSql.Data.Planners.Index;
using PrimarSql.Data.Planners.Table;
using PrimarSql.Data.Sources;
using PrimarSql.Data.Utilities;
using static PrimarSql.Internal.PrimarSqlParser;
using static PrimarSql.Data.Utilities.ValidateUtility;

namespace PrimarSql.Data.Visitors
{
    internal static class ContextVisitor
    {
        public static IQueryPlanner Visit(RootContext context)
        {
            return VisitSqlStatement(context.sqlStatement());
        }

        public static IQueryPlanner VisitSqlStatement(SqlStatementContext context)
        {
            if (context.dmlStatement() != null)
                return VisitDmlStatementContext(context.dmlStatement());

            if (context.ddlStatement() != null)
                return VisitDdlStatementContext(context.ddlStatement());

            // describe

            // show

            return null;
        }

        #region DML Statement
        public static IQueryPlanner VisitDmlStatementContext(DmlStatementContext context)
        {
            if (context.children.Count == 0)
                return null;

            switch (context.children[0])
            {
                case SelectStatementContext selectStatementContext:
                    return new SelectQueryPlanner(VisitSelectStatement(selectStatementContext));

                    ;

                case InsertStatementContext insertStatementContext:

                    break;

                case UpdateStatementContext updateStatementContext:

                    break;

                case DeleteStatementContext deleteStatementContext:
                    break;
            }

            return null;
        }

        public static SelectQueryInfo VisitSelectStatement(SelectStatementContext context)
        {
            switch (context)
            {
                case SimpleSelectContext simpleSelectContext:
                    return VisitSimpleSelectContext(simpleSelectContext);

                case ParenthesisSelectContext parenthesisSelectContext:
                    return VisitQueryExpression(parenthesisSelectContext.queryExpression());
            }

            return null;
        }

        public static SelectQueryInfo VisitQueryExpression(QueryExpressionContext context)
        {
            if (context.queryExpression() != null)
                return VisitQueryExpression(context.queryExpression());

            return VisitQuerySpecification(context.querySpecification());
        }

        public static SelectQueryInfo VisitSimpleSelectContext(SimpleSelectContext context)
        {
            return VisitQuerySpecification(context.querySpecification());
        }

        public static SelectQueryInfo VisitQuerySpecification(QuerySpecificationContext context)
        {
            var queryInfo = new SelectQueryInfo();

            var fromClause = context.fromClause();

            if (context.selectSpec() != null)
            {
                if (context.selectSpec().STRONGLY() != null)
                    queryInfo.UseStronglyConsistent = true;
            }

            queryInfo.Columns = VisitSelectElements(context.selectElements());

            if (fromClause == null)
            {
                queryInfo.TableSource = null;
            }
            else
            {
                queryInfo.TableSource = VisitTableSource(fromClause.tableSource());

                if (fromClause.whereExpr != null)
                    queryInfo.WhereExpression = ExpressionVisitor.VisitExpression(fromClause.whereExpr);
            }

            if (context.orderClause() != null)
            {
                if (context.orderClause().DESC() != null)
                    queryInfo.OrderDescend = true;
            }

            if (context.limitClause() != null)
            {
                var limitClause = context.limitClause();

                if (int.TryParse(limitClause.limit.GetText(), out int limit))
                    queryInfo.Limit = limit;
            }

            if (context.offsetClause() != null)
            {
                var offsetClause = context.offsetClause();

                if (int.TryParse(offsetClause.offset.GetText(), out int offset))
                    queryInfo.Offset = offset;
            }

            if (context.startKeyClause() != null)
            {
                var startKeyClause = context.startKeyClause();

                queryInfo.StartHashKey = ExpressionVisitor.VisitConstant(startKeyClause.hashKey);

                if (startKeyClause.sortKey != null)
                {
                    queryInfo.StartSortKey = ExpressionVisitor.VisitConstant(startKeyClause.sortKey);
                }
            }

            return queryInfo;
        }

        public static IColumn[] VisitSelectElements(SelectElementsContext context)
        {
            if (context.star != null)
            {
                return new IColumn[]
                {
                    new StarColumn()
                };
            }

            return context.selectElement().Select(VisitSelectElement).ToArray();
        }

        public static IColumn VisitSelectElement(SelectElementContext context)
        {
            switch (context)
            {
                case SelectColumnElementContext selectColumnElementContext:
                    return new PropertyColumn
                    {
                        Name = IdentifierUtility.Parse(selectColumnElementContext.fullColumnName().GetText()),
                        Alias = IdentifierUtility.Unescape(selectColumnElementContext.alias?.GetText()),
                    };

                case SelectFunctionElementContext selectFunctionElementContext:
                    throw new NotSupportedException("Not Supported Select Element Function Feature.");

                case SelectExpressionElementContext selectExpressionElementContext:
                    throw new NotSupportedException("Not Supported Select Element Expression Feature.");
            }

            return null;
        }

        public static ITableSource VisitTableSource(TableSourceContext context)
        {
            switch (context)
            {
                case TableSourceBaseContext tableSourceBaseContext:
                    return VisitTableSourceBase(tableSourceBaseContext.tableSourceItem());

                case TableSourceNestedContext tableSourceNestedContext:
                    return VisitTableSourceBase(tableSourceNestedContext.tableSourceItem());
            }

            return null;
        }

        public static ITableSource VisitTableSourceBase(TableSourceItemContext context)
        {
            switch (context)
            {
                case AtomTableItemContext atomTableItemContext:
                {
                    IPart[] identifiers = IdentifierUtility.Parse(atomTableItemContext.tableName().GetText());
                    ValidateTableWithIndexName(identifiers);

                    return new AtomTableSource(
                        identifiers[0].ToString(),
                        identifiers.Length == 2 ? identifiers[1].ToString() : string.Empty
                    );
                }

                case SubqueryTableItemContext subqueryTableItemContext:
                    return new SubquerySource
                    {
                        SubqueryInfo = VisitSelectStatement(subqueryTableItemContext.parenthesisSubquery)
                    };
            }

            return null;
        }
        #endregion

        public static IQueryPlanner VisitDdlStatementContext(DdlStatementContext context)
        {
            if (context.children.Count == 0)
                return null;

            switch (context.children[0])
            {
                case CreateIndexContext createIndexContext:

                    break;

                case CreateTableContext createTableContext:
                    return VisitCreateTableContext(createTableContext);

                case AlterTableContext alterTableContext:
                    return VisitAlterTableContext(alterTableContext);

                case DropIndexContext dropIndexContext:
                    break;

                case DropTableContext dropTableContext:
                    return VisitDropTableContext(dropTableContext);
            }

            return null;
        }

        #region Create Table
        public static CreateTablePlanner VisitCreateTableContext(CreateTableContext context)
        {
            var queryInfo = new CreateTableQueryInfo
            {
                TableName = VisitTableName(context.tableName()),
                SkipIfExists = context.ifNotExists() != null
            };

            VisitCreateDefinitions(context.createDefinitions(), queryInfo);

            foreach (var tableOption in context.tableOption())
                VisitTableOption(tableOption, queryInfo);

            var planner = new CreateTablePlanner
            {
                QueryInfo = queryInfo
            };

            return planner;
        }

        public static void VisitCreateDefinitions(CreateDefinitionsContext context, CreateTableQueryInfo queryInfo)
        {
            var columns = new Dictionary<string, TableColumn>();
            var constraints = new Dictionary<string, bool?>();
            var indexes = new Dictionary<string, IndexDefinition>();

            foreach (var definition in context.createDefinition())
            {
                switch (definition)
                {
                    case ColumnDeclarationContext columnDeclarationContext:
                    {
                        var tableColumn = VisitColumnDeclaration(columnDeclarationContext);

                        if (columns.ContainsKey(tableColumn.ColumnName))
                            throw new InvalidOperationException($"Column name {tableColumn.ColumnName} duplicate.");

                        columns[tableColumn.ColumnName] = tableColumn;
                        break;
                    }

                    case ConstraintDeclarationContext constraintDeclarationContext:
                    {
                        (string columnName, bool? isHashKey) = VisitTableConstraint(constraintDeclarationContext.tableConstraint());

                        if (constraints.ContainsKey(columnName))
                            throw new InvalidOperationException($"Column name {columnName} duplicate.");

                        constraints[columnName] = isHashKey;
                        break;
                    }

                    case IndexDeclarationContext indexColumnDefinitionContext:
                    {
                        var indexDefinition = VisitIndexColumnDefinition(indexColumnDefinitionContext.indexColumnDefinition());

                        if (indexes.ContainsKey(indexDefinition.IndexName))
                            throw new InvalidOperationException($"Index name {indexDefinition.IndexName} duplicate.");

                        indexes[indexDefinition.IndexName] = indexDefinition;
                        break;
                    }
                }
            }

            // Column constraint validation
            foreach ((var columnName, bool? isHashKey) in constraints)
            {
                if (!columns.ContainsKey(columnName))
                    throw new InvalidOperationException($"Column name {columnName} not defined.");

                var tableColumn = columns[columnName];

                if (tableColumn.IsHashKey || tableColumn.IsSortKey)
                    throw new InvalidOperationException($"Already {columnName} constraint is defined.");

                tableColumn.IsHashKey = isHashKey ?? false;
                tableColumn.IsSortKey = !isHashKey ?? false;
            }

            // Index columns validation
            foreach ((string indexName, var indexDefinition) in indexes)
            {
                if (!columns.ContainsKey(indexDefinition.HashKey) || (!string.IsNullOrWhiteSpace(indexDefinition.SortKey) && !columns.ContainsKey(indexDefinition.SortKey)))
                    throw new InvalidOperationException($"{indexName} use not defined column name.");
            }

            queryInfo.TableColumns = columns.Select(kv => kv.Value).ToArray();
            queryInfo.IndexDefinitions = indexes.Select(kv => kv.Value).ToArray();

            // HashKey, SortKey Validation         

            if (queryInfo.TableColumns.Count(column => column.IsHashKey) != 1)
                throw new InvalidOperationException($"No hash key defined for Table {queryInfo.TableName}.");

            if (queryInfo.TableColumns.Count(column => column.IsSortKey) > 1)
                throw new InvalidOperationException($"Too many sort key defined for Table {queryInfo.TableName}.");
        }
        #endregion

        #region Alter Table
        public static AlterTablePlanner VisitAlterTableContext(AlterTableContext context)
        {
            var queryInfo = new AlterTableQueryInfo();

            return new AlterTablePlanner
            {
                QueryInfo = queryInfo
            };
        }
        #endregion

        public static void VisitTableOption(TableOptionContext context, CreateTableQueryInfo queryInfo)
        {
            switch (context)
            {
                case TableOptionThroughputContext tableOptionThroughputContext:
                    if (int.TryParse(tableOptionThroughputContext.readCapacity.GetText(), out int readCapacity))
                        queryInfo.ReadCapacity = readCapacity;

                    if (int.TryParse(tableOptionThroughputContext.writeCapacity.GetText(), out int writeCapacity))
                        queryInfo.WriteCapacity = writeCapacity;

                    break;

                case TableBillingModeContext tableBillingModeContext:
                    if (tableBillingModeContext.PROVISIONED() != null)
                        queryInfo.TableBillingMode = TableBillingMode.Provisoned;

                    if (tableBillingModeContext.PAY_PER_REQUEST() != null)
                        queryInfo.TableBillingMode = TableBillingMode.PayPerRequest;

                    if (tableBillingModeContext.ON_DEMAND() != null)
                        queryInfo.TableBillingMode = TableBillingMode.PayPerRequest;

                    break;
            }
        }

        public static TableColumn VisitColumnDeclaration(ColumnDeclarationContext context)
        {
            var columnName = GetSinglePartName(context.uid().GetText(), "Column");
            (string dataType, bool? isHashKey) = VisitColumnDefinition(context.columnDefinition());

            return new TableColumn
            {
                ColumnName = columnName,
                DataType = dataType,
                IsHashKey = isHashKey ?? false,
                IsSortKey = !isHashKey ?? false,
            };
        }

        public static (string dataType, bool? isHashKey) VisitColumnDefinition(ColumnDefinitionContext context)
        {
            bool? isHashKey = null;

            if (context.columnConstraint() != null)
                isHashKey = IsHashKey(context.columnConstraint());

            return (context.dataType().GetText(), isHashKey);
        }

        public static (string name, bool? isHashKey) VisitTableConstraint(TableConstraintContext context)
        {
            var columnName = GetSinglePartName(context.uid().GetText(), "Column");

            return (columnName, IsHashKey(context.columnConstraint()));
        }

        public static bool IsHashKey(ColumnConstraintContext context)
        {
            return context switch
            {
                HashKeyColumnConstraintContext _ => true,
                RangeKeyColumnConstraintContext _ => false,
                _ => throw new NotSupportedException($"Not Supported Column constraint {context?.GetType().Name ?? "Unknown Context"}.")
            };
        }

        public static IndexDefinition VisitIndexColumnDefinition(IndexColumnDefinitionContext context)
        {
            var definition = new IndexDefinition();

            if (context.indexSpec() != null)
                definition.IsLocalIndex = context.indexSpec().LOCAL() != null;

            definition.IndexName = GetSinglePartName(context.uid().GetText(), "Index");

            (string hashKey, string sortKey) = VisitPrimaryKeyColumns(context.primaryKeyColumns());

            definition.HashKey = hashKey;
            definition.SortKey = sortKey;

            definition.IndexType = VisitIndexOption(context.indexOption());

            if (definition.IndexType == IndexType.Include)
                definition.IncludeColumns = VisitIndexOptionToGetIncludeColumns(context.indexOption());

            return definition;
        }

        public static IndexType VisitIndexOption(IndexOptionContext context)
        {
            if (context.ALL() != null)
                return IndexType.All;

            if (context.KEYS() != null && context.ONLY() != null)
                return IndexType.KeysOnly;

            if (context.INCLUDE() != null)
                return IndexType.Include;

            throw new NotSupportedException($"Not supported {context.GetText()} index type.");
        }

        public static string[] VisitIndexOptionToGetIncludeColumns(IndexOptionContext context)
        {
            return context.uid()
                .Select(id => GetSinglePartName(id.GetText(), "Column"))
                .ToArray();
        }

        public static DropTablePlanner VisitDropTableContext(DropTableContext context)
        {
            IEnumerable<string> targetTables = context.tableName().Select(VisitTableName);

            var dropTablePlanner = new DropTablePlanner
            {
                QueryInfo = new DropTableQueryInfo(targetTables)
            };

            return dropTablePlanner;
        }

        public static (string hashKey, string sortKey) VisitPrimaryKeyColumns(PrimaryKeyColumnsContext context)
        {
            return (context.hashKey?.GetText(), context.sortKey?.GetText());
        }

        public static string VisitTableName(TableNameContext context)
        {
            return GetSinglePartName(context.GetText(), "Table");
        }

        public static string GetSinglePartName(string text, string nameType)
        {
            IPart[] parts = IdentifierUtility.Parse(text);
            ValidateSingleName(parts, nameType);

            return parts.FirstOrDefault()?.ToString();
        }
    }
}
