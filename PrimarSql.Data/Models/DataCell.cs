using System;

namespace PrimarSql.Data.Models
{
    public class DataCell
    {
        public string Name { get; set; }
        
        public Type DataType { get; set; }
        
        public string TypeName { get; set; } 
        
        public object Data { get; set; }
    }
}
