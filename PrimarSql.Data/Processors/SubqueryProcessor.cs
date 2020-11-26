using System;
using System.Collections.Generic;
using System.Data;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models.Columns;

namespace PrimarSql.Data.Processors
{
    internal sealed class SubqueryProcessor : BaseProcessor
    {
        private readonly IProcessor _sourceProcessor;
        private readonly IProcessor _processor;

        public override Dictionary<string, AttributeValue> Current
        {
            get => _sourceProcessor.Current;
            set => throw new NotSupportedException("SubqueryProcessor cannot set Current");
        }

        public override IColumn[] Columns => _processor.Columns;

        public SubqueryProcessor(IProcessor processor, IProcessor sourceProcessor)
        {
            _sourceProcessor = sourceProcessor;
            _processor = processor;
        }

        public override DataTable GetSchemaTable()
        {
            if (_processor is StarProcessor)
                return _sourceProcessor.GetSchemaTable();

            return _processor.GetSchemaTable();
        }

        public override object[] Process()
        {
            if (_processor is StarProcessor)
                return _sourceProcessor.Process();

            _processor.Current = _sourceProcessor.Filter();
            return _processor.Process();
        }

        public override Dictionary<string, AttributeValue> Filter()
        {
            if (_processor is StarProcessor)
                return _sourceProcessor.Filter();

            _processor.Current = _sourceProcessor.Filter();
            return _processor.Filter();
        }
    }
}
