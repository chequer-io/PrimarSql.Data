namespace PrimarSql.Data.Models
{
    public interface IKey
    {
        public int StartToken { get; set; }
        
        public int EndToken { get; set; }
    }
}
