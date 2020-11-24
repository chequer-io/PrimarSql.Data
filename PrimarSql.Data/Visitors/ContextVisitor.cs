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
            if (context == null)
                return null;

            if (context.dmlStatement() != null)
                return VisitDmlStatementContext(context.dmlStatement());

            if (context.ddlStatement() != null)
                return VisitDdlStatementContext(context.ddlStatement());

            if (context.describeStatement() != null)
                return VisitDescribeStatementContext(context.describeStatement());

            if (context.showStatement() != null)
                return VisitShowStatementContext(context.showStatement());

            throw new NotSupportedException($"Not Supported Statement. (Name: {context.GetType().Name[..^7]})");
        }

        #region DML Statement
        public static IQueryPlanner VisitDmlStatementContext(DmlStatementContext context)
        {
            if (context.children.Count == 0)
                return null;

            switch (context.children[0])
            {
                case SelectStatementContext selectStatementContext:
                    return new SelectPlanner(VisitSelectStatement(selectStatementContext));

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

                case SelectFunctionElementContext _:
                    throw new NotSupportedException("Not Supported Select Element Function Feature.");

                case SelectExpressionElementContext _:
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
                    return VisitCreateIndexContext(createIndexContext);

                case CreateTableContext createTableContext:
                    return VisitCreateTableContext(createTableContext);

                case AlterTableContext alterTableContext:
                    return VisitAlterTableContext(alterTableContext);

                case DropIndexContext dropIndexContext:
                    return VisitDropIndexContext(dropIndexContext);

                case DropTableContext dropTableContext:
                    return VisitDropTableContext(dropTableContext);
            }

            return null;
        }

        #region Create Index
        public static CreateIndexPlanner VisitCreateIndexContext(CreateIndexContext context)
        {
            return new CreateIndexPlanner
            {
                QueryInfo = new CreateIndexQueryInfo
                {
                    IndexDefinitionWithType = CreateIndexDefinition(context.uid(), context.indexSpec(), context.indexOption(), context.primaryKeyColumnsWithType()),
                    TableName = VisitTableName(context.tableName())
                }
            };
        }
        #endregion

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
            foreach (var definition in context.createDefinition())
            {
                switch (definition)
                {
                    case ColumnDeclarationContext columnDeclarationContext:
                    {
                        var tableColumn = VisitColumnDeclaration(columnDeclarationContext);
                        queryInfo.AddTableColumn(tableColumn);
                        break;
                    }

                    case ConstraintDeclarationContext constraintDeclarationContext:
                    {
                        (string columnName, bool isHashKey) = VisitTableConstraint(constraintDeclarationContext.tableConstraint());
                        queryInfo.SetConstraint(columnName, isHashKey);
                        break;
                    }

                    case IndexDeclarationContext indexColumnDefinitionContext:
                    {
                        var indexDefinition = VisitIndexColumnDefinition(indexColumnDefinitionContext.indexColumnDefinition());
                        queryInfo.AddIndexDefinition(indexDefinition);
                        break;
                    }
                }
            }
        }
        #endregion

        #region Alter Table
        public static AlterTablePlanner VisitAlterTableContext(AlterTableContext context)
        {
            var queryInfo = new AlterTableQueryInfo
            {
                TableName = VisitTableName(context.tableName())
            };

            VisitAlterSpecification(context.alterSpecification(), queryInfo);

            return new AlterTablePlanner
            {
                QueryInfo = queryInfo
            };
        }

        public static void VisitAlterSpecification(IEnumerable<AlterSpecificationContext> contexts, AlterTableQueryInfo queryInfo)
        {
            foreach (var alterSpecificationContext in contexts)
            {
                switch (alterSpecificationContext)
                {
                    case AlterTableOptionContext alterTableOptionContext:
                        VisitTableOption(alterTableOptionContext.tableOption(), queryInfo);
                        break;

                    case AlterByAddColumnContext alterByAddColumnContext:
                        queryInfo.AddTableColumn(VisitAlterByAddColumnContext(alterByAddColumnContext.uid(), alterByAddColumnContext.dataType()));
                        break;

                    case AlterByAddColumnsContext alterByAddColumnsContext:
                        int i = 0;

                        foreach (var uid in alterByAddColumnsContext.uid())
                        {
                            queryInfo.AddTableColumn(VisitAlterByAddColumnContext(uid, alterByAddColumnsContext.dataType(i)));
                            i++;
                        }

                        break;

                    case AlterAddIndexContext alterAddIndexContext:
                        var indexDefinition = VisitIndexColumnDefinition(alterAddIndexContext.indexColumnDefinition());
                        queryInfo.AddIndexAddAction(indexDefinition);
                        break;

                    case AlterIndexThroughputContext alterIndexThroughputContext:
                        if (!int.TryParse(alterIndexThroughputContext.readCapacity.GetText(), out int readCapacity))
                            throw new InvalidOperationException("Read capacity cannot be null.");

                        if (!int.TryParse(alterIndexThroughputContext.writeCapacity.GetText(), out int writeCapacity))
                            throw new InvalidOperationException("Write capacity cannot be null.");

                        var indexName = GetSinglePartName(alterIndexThroughputContext.uid().GetText(), "Index");
                        queryInfo.AddIndexAlterAction(indexName, readCapacity, writeCapacity);
                        break;
                }
            }
        }

        public static TableColumn VisitAlterByAddColumnContext(UidContext uidContext, DataTypeContext dataTypeContext)
        {
            return new TableColumn
            {
                ColumnName = GetSinglePartName(uidContext.GetText(), "Column"),
                DataType = dataTypeContext.GetText()
            };
        }
        #endregion

        public static void VisitTableOption(TableOptionContext context, TableQueryInfo queryInfo)
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

        public static KeyTableColumn VisitColumnDeclaration(ColumnDeclarationContext context)
        {
            var columnName = GetSinglePartName(context.uid().GetText(), "Column");
            (string dataType, bool? isHashKey) = VisitColumnDefinition(context.columnDefinition());

            return new KeyTableColumn
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

        public static (string name, bool isHashKey) VisitTableConstraint(TableConstraintContext context)
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

        public static IndexDefinitionWithType CreateIndexDefinition(UidContext indexName, IndexSpecContext indexSpec, IndexOptionContext indexOption, PrimaryKeyColumnsWithTypeContext primaryKeyColumnsWithTypeContext)
        {
            var definition = CreateIndexDefinition(indexName, indexSpec, indexOption);

            (string hashKey, string hashKeyType, string sortKey, string sortKeyType) = VisitPrimaryKeyWithTypeColumns(primaryKeyColumnsWithTypeContext);

            definition.HashKey = hashKey;
            definition.SortKey = sortKey;

            return new IndexDefinitionWithType
            {
                IndexDefinition = definition,
                HashKeyType = hashKeyType,
                SortKeyType = sortKeyType,
            };
        }

        public static IndexDefinition CreateIndexDefinition(UidContext indexName, IndexSpecContext indexSpec, IndexOptionContext indexOption, PrimaryKeyColumnsContext primaryKeyColumns)
        {
            var definition = CreateIndexDefinition(indexName, indexSpec, indexOption);

            (string hashKey, string sortKey) = VisitPrimaryKeyColumns(primaryKeyColumns);

            definition.HashKey = hashKey;
            definition.SortKey = sortKey;

            return definition;
        }

        public static IndexDefinition CreateIndexDefinition(UidContext indexName, IndexSpecContext indexSpec, IndexOptionContext indexOption)
        {
            var definition = new IndexDefinition();

            if (indexSpec != null)
                definition.IsLocalIndex = indexSpec.LOCAL() != null;

            definition.IndexName = GetSinglePartName(indexName.GetText(), "Index");

            if (indexOption != null)
                definition.IndexType = VisitIndexOption(indexOption);

            if (definition.IndexType == IndexType.Include)
                definition.IncludeColumns = VisitIndexOptionToGetIncludeColumns(indexOption);

            return definition;
        }

        public static IndexDefinition VisitIndexColumnDefinition(IndexColumnDefinitionContext context)
        {
            return CreateIndexDefinition(context.uid(), context.indexSpec(), context.indexOption(), context.primaryKeyColumns());
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

        public static DropIndexPlanner VisitDropIndexContext(DropIndexContext context)
        {
            string indexName = GetSinglePartName(context.indexName.GetText(), "Index Name");
            string tableName = VisitTableName(context.tableName());

            var dropIndexPlanner = new DropIndexPlanner
            {
                QueryInfo = new DropIndexQueryInfo(tableName, indexName)
            };

            return dropIndexPlanner;
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

        public static (string hashKey, string hashKeyType, string sortKey, string sortKeyType) VisitPrimaryKeyWithTypeColumns(PrimaryKeyColumnsWithTypeContext context)
        {
            return (context.hashKey?.GetText(), context.hashKeyType?.GetText(),
                context.sortKey?.GetText(), context.sortKeyType?.GetText());
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

        public static IQueryPlanner VisitDescribeStatementContext(DescribeStatementContext context)
        {
            return VisitDescribeSpecificationContext(context.describeSpecification());
        }

        public static IQueryPlanner VisitDescribeSpecificationContext(DescribeSpecificationContext context)
        {
            switch (context)
            {
                case DescribeTableContext describeTableContext:
                    break;

                case DescribeLimitsContext describeLimitsContext:
                    break;

                case DescribeEndPointsContext describeEndPointsContext:
                    break;
            }

            return null;
        }

        public static IQueryPlanner VisitShowStatementContext(ShowStatementContext context)
        {
            return VisitShowSpecification(context.showSpecification());
        }

        public static IQueryPlanner VisitShowSpecification(ShowSpecificationContext context)
        {
            switch (context)
            {
                case ShowTablesContext showTablesContext:
                    break;

                case ShowIndexesContext showIndexesContext:
                    break;
            }

            return null;
        }
    }
}
