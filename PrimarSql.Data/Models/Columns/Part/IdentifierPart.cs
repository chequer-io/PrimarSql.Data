namespace PrimarSql.Data.Models.Columns
{
    internal sealed class IdentifierPart : IPart
    {
        public string Identifier { get; }
        
        public IdentifierPart(string identifier)
        {
            Identifier = identifier;
        }

        public override string ToString()
        {
            return Identifier;
        }
    }
}
