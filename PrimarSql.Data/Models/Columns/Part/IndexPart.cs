namespace PrimarSql.Data.Models.Columns
{
    internal sealed class IndexPart : IPart
    {
        public int Index { get; }

        public IndexPart(int index)
        {
            Index = index;
        }
    }
}
