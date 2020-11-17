using System.Linq;
using PrimarSql.Data.Models;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Planners;
using PrimarSql.Data.Sources;
using PrimarSql.Data.Utilities;
using static PrimarSql.Internal.PrimarSqlParser;
using static PrimarSql.Data.Utilities.Validator;

namespace PrimarSql.Data.Processors
{
    // TODO: internal
    public class ContextProcessor
    {
        public QueryPlanner Process(RootContext context)
        {
            var statement = context
                .sqlStatements()
                .sqlStatement(0);

            if (statement == null || statement.children.Count == 0)
                return null;

            switch (statement.children[0])
            {
                case DmlStatementContext dmlStatementContext:
                    return ProcessDmlStatementContext(dmlStatementContext);

                case DdlStatementContext ddlStatementContext:
                    return ProcessDdlStatementContext(ddlStatementContext);
            }

            return null;
        }

        #region DML Statement
        public QueryPlanner ProcessDmlStatementContext(DmlStatementContext context)
        {
            if (context.children.Count == 0)
                return null;

            switch (context.children[0])
            {
                case SelectStatementContext selectStatementContext:
                    return ProcessSelectStatementContext(selectStatementContext);

                case InsertStatementContext insertStatementContext:

                    break;

                case UpdateStatementContext updateStatementContext:

                    break;

                case DeleteStatementContext deleteStatementContext:
                    break;
            }

            return null;
        }

        public SelectQueryPlanner ProcessSelectStatementContext(SelectStatementContext context)
        {
            switch (context)
            {
                case SimpleSelectContext simpleSelectContext:
                    return ProcessSimpleSelectContext(simpleSelectContext);

                case ParenthesisSelectContext parenthesisSelectContext:
                    break;
            }

            return null;
        }

        public SelectQueryPlanner ProcessSimpleSelectContext(SimpleSelectContext context)
        {
            return ProcessQuerySpecification(context.querySpecification());
        }

        public SelectQueryPlanner ProcessQuerySpecification(QuerySpecificationContext context)
        {
            var planner = new SelectQueryPlanner();
            var queryInfo = new SelectQueryInfo();

            var fromClause = context.fromClause();

            if (context.selectSpec() != null)
            {
                if (context.selectSpec().STRONGLY() != null)
                    queryInfo.UseStronglyConsistent = true;
            }

            queryInfo.Columns = ProcessSelectElements(context.selectElements());

            if (fromClause == null)
            {
                queryInfo.TableSource = null;
            }
            else
            {
                queryInfo.TableSource = ProcessTableSource(fromClause.tableSource());

                if (fromClause.whereExpr != null)
                {
                    var whereExpr = fromClause.whereExpr;
                }
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

            planner.QueryInfo = queryInfo;

            return planner;
        }

        public IColumn[] ProcessSelectElements(SelectElementsContext context)
        {
            return new IColumn[]
            {
                new StarColumn()
            };
        }

        public ITableSource ProcessTableSource(TableSourceContext context)
        {
            switch (context)
            {
                case TableSourceBaseContext tableSourceBaseContext:
                    return ProcessTableSourceBase(tableSourceBaseContext.tableSourceItem());

                case TableSourceNestedContext tableSourceNestedContext:
                    return ProcessTableSourceBase(tableSourceNestedContext.tableSourceItem());
            }

            return null;
        }

        public ITableSource ProcessTableSourceBase(TableSourceItemContext context)
        {
            switch (context)
            {
                case AtomTableItemContext atomTableItemContext:
                    return new AtomTableSource(IdentifierUtility.Parse(atomTableItemContext.tableName().GetText())[0]);

                case SubqueryTableItemContext subqueryTableItemContext:
                    return new SubquerySource
                    {
                        SubqueryInfo = ProcessSelectStatementContext(subqueryTableItemContext.parenthesisSubquery).QueryInfo
                    };
            }

            return null;
        }
        #endregion

        public QueryPlanner ProcessDdlStatementContext(DdlStatementContext context)
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
                    return ProcessDropTableContext(dropTableContext);
            }

            return null;
        }

        public DropTablePlanner ProcessDropTableContext(DropTableContext context)
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
