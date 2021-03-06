﻿namespace PrimarSql.Data.Models
{
    internal sealed class HashKey : IKey
    {
        public ExpressionAttributeName ExpressionAttributeName { get; set; }

        public ExpressionAttributeValue ExpressionAttributeValue { get; set; }

        public override string ToString()
        {
            return $"{ExpressionAttributeName.Key} = {ExpressionAttributeValue.Key}";
        }
    }
}
