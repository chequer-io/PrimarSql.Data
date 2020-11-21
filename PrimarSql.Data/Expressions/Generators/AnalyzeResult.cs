using PrimarSql.Data.Models;

namespace PrimarSql.Data.Expressions.Generators
{
    public class AnalyzeResult
    {
        public static readonly AnalyzeResult Success = new AnalyzeResult(); 
        
        public AnalyzeState State { get; set; }

        public IKey Key { get; set; }
        
        public AnalyzeResult()
        {
            State = AnalyzeState.Success;
        }
        
        public AnalyzeResult(HashKey hashKey)
        {
            State = AnalyzeState.HashKey;
            Key = hashKey;
        }
        
        public AnalyzeResult(SortKey sortKey)
        {
            State = AnalyzeState.SortKey;
            Key = sortKey;
        }
    }
}
