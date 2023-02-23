using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace PrimarSql.Data.Models;

public class JDynamoDBNumber : JValue
{
    public JDynamoDBNumber(string value) : base(value)
    {
    }

    public override async Task WriteToAsync(JsonWriter writer, CancellationToken cancellationToken, params JsonConverter[] converters)
    {
        await writer.WriteRawValueAsync(Value?.ToString(), cancellationToken);
    }

    public override void WriteTo(JsonWriter writer, params JsonConverter[] converters)
    {
        writer.WriteRawValue(Value?.ToString());
    }
}
