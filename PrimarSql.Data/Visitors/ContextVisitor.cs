using System.Linq;
using PrimarSql.Data.Models;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Planners;
using PrimarSql.Data.Sources;
using PrimarSql.Data.Utilities;
using static PrimarSql.Internal.PrimarSqlParser;
using static PrimarSql.Data.Utilities.Validator;

namespace PrimarSql.Data.Visitors
{
    // TODO: internal
    public static class ContextVisitor
    {
        public static QueryPlanner Visit(RootContext context)
        {
            return VisitSqlStatement(context.sqlStatement());
        }

        public static QueryPlanner VisitSqlStatement(SqlStatementContext context)
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
        public static QueryPlanner VisitDmlStatementContext(DmlStatementContext context)
        {
            if (context.children.Count == 0)
                return null;

            switch (context.children[0])
            {
                case SelectStatementContext selectStatementContext:
                    return new SelectQueryPlanner(VisitSelectStatement(selectStatementContext));;

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

                if (long.TryParse(limitClause.limit.GetText(), out long limit))
                    queryInfo.Limit = limit;
            }

            if (context.offsetClause() != null)
            {
                var offsetClause = context.offsetClause();

                if (long.TryParse(offsetClause.offset.GetText(), out long offset))
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
            return new IColumn[]
            {
                new StarColumn()
            };
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
                    string[] identifiers = IdentifierUtility.Parse(atomTableItemContext.tableName().GetText());
                    ValidateTableWithIndexName(identifiers);

                    return new AtomTableSource(
                        identifiers[0],
                        identifiers.Length == 2 ? identifiers[1] : string.Empty
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

        public static QueryPlanner VisitDdlStatementContext(DdlStatementContext context)
        {
            if (context.children.Count == 0)
                return null;

            switch (context.children[0])
            {
                case CreateIndexContext createIndexContext:

                    break;

                case CreateTableContext createTableContext:

                    break;

                case AlterTableContext alterTableContext:

                    break;

                case DropIndexContext dropIndexContext:
                    break;

                case DropTableContext dropTableContext:
                    return VisitDropTableContext(dropTableContext);
            }

            return null;
        }

        public static DropTablePlanner VisitDropTableContext(DropTableContext context)
        {
            var dropTablePlanner = new DropTablePlanner
            {
                TargetTables = context.tableName().Select(tableName =>
                {
                    string[] result = IdentifierUtility.Parse(tableName.GetText());
                    ValidateTableName(result);

                    return result.FirstOrDefault();
                }).ToArray()
            };

            return dropTablePlanner;
        }
    }
}
