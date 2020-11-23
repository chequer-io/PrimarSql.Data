using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data.Planners.Index
{
    internal sealed class IndexDefinition
    {
        private Projection _projection;
        private List<KeySchemaElement> _keySchema;
        
        public bool IsLocalIndex { get; set; }
        
        public string IndexName { get; set; }

        public IndexType IndexType { get; set; }
        
        public string[] IncludeColumns { get; set; }
        
        public string HashKey { get; set; }
        
        public string SortKey { get; set; }

        public Projection Projection
        {
            get
            {
                return _projection ??= new Projection
                {
                    ProjectionType = IndexType switch
                    {
                        IndexType.All => ProjectionType.ALL,
                        IndexType.Include => ProjectionType.INCLUDE,
                        IndexType.KeysOnly => ProjectionType.KEYS_ONLY,
                        _ => ProjectionType.ALL
                    },
                    NonKeyAttributes = IndexType != IndexType.Include ? null : IncludeColumns.ToList(),
                };
            }
        }

        public List<KeySchemaElement> KeySchema
        {
            get
            {
                if (_keySchema == null)
                {
                    _keySchema = new List<KeySchemaElement>
                    {
                        new KeySchemaElement(HashKey, KeyType.HASH)
                    };

                    if (!string.IsNullOrWhiteSpace(SortKey))
                        _keySchema.Add(new KeySchemaElement(SortKey, KeyType.RANGE));    
                }

                return _keySchema;
            }
        }
    }
}
