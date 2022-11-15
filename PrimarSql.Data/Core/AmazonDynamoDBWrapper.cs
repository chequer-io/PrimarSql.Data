using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace PrimarSql.Data.Core
{
    internal readonly struct AmazonDynamoDBWrapper : IAmazonDynamoDB
    {
        public IClientConfig Config => _db.Config;

        public IDynamoDBv2PaginatorFactory Paginators => _db.Paginators;

        private readonly IAmazonDynamoDB _db;

        public AmazonDynamoDBWrapper(IAmazonDynamoDB db)
        {
            _db = db;
        }

        public async Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.BatchExecuteStatementAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, ReturnConsumedCapacity returnConsumedCapacity, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.BatchGetItemAsync(requestItems, returnConsumedCapacity, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.BatchGetItemAsync(requestItems, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.BatchGetItemAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<BatchWriteItemResponse> BatchWriteItemAsync(Dictionary<string, List<WriteRequest>> requestItems, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.BatchWriteItemAsync(requestItems, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<BatchWriteItemResponse> BatchWriteItemAsync(BatchWriteItemRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.BatchWriteItemAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.CreateBackupAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.CreateGlobalTableAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<CreateTableResponse> CreateTableAsync(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.CreateTableAsync(tableName, keySchema, attributeDefinitions, provisionedThroughput, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<CreateTableResponse> CreateTableAsync(CreateTableRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.CreateTableAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DeleteBackupAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DeleteItemAsync(tableName, key, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, ReturnValue returnValues, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DeleteItemAsync(tableName, key, returnValues, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DeleteItemResponse> DeleteItemAsync(DeleteItemRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DeleteItemAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DeleteTableResponse> DeleteTableAsync(string tableName, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DeleteTableAsync(tableName, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DeleteTableResponse> DeleteTableAsync(DeleteTableRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DeleteTableAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeBackupAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeContinuousBackupsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeContributorInsightsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeEndpointsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeExportAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeGlobalTableAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeGlobalTableSettingsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeImportAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeKinesisStreamingDestinationAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeLimitsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeTableResponse> DescribeTableAsync(string tableName, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeTableAsync(tableName, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeTableAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeTableReplicaAutoScalingAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeTimeToLiveAsync(tableName, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DescribeTimeToLiveAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.DisableKinesisStreamingDestinationAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.EnableKinesisStreamingDestinationAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ExecuteStatementAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ExecuteTransactionAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ExportTableToPointInTimeAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.GetItemAsync(tableName, key, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, bool consistentRead, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.GetItemAsync(tableName, key, consistentRead, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.GetItemAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ImportTableAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ListBackupsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ListContributorInsightsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ListExportsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ListGlobalTablesAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ListImportsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ListTablesResponse> ListTablesAsync(CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ListTablesAsync(cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ListTablesAsync(exclusiveStartTableName, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, int limit, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ListTablesAsync(exclusiveStartTableName, limit, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ListTablesResponse> ListTablesAsync(int limit, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ListTablesAsync(limit, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ListTablesResponse> ListTablesAsync(ListTablesRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ListTablesAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ListTagsOfResourceAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.PutItemAsync(tableName, item, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, ReturnValue returnValues, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.PutItemAsync(tableName, item, returnValues, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.PutItemAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.QueryAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.RestoreTableFromBackupAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.RestoreTableToPointInTimeAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ScanAsync(tableName, attributesToGet, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ScanResponse> ScanAsync(string tableName, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ScanAsync(tableName, scanFilter, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ScanAsync(tableName, attributesToGet, scanFilter, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.ScanAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.TagResourceAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.TransactGetItemsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.TransactWriteItemsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.UntagResourceAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.UpdateContinuousBackupsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.UpdateContributorInsightsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.UpdateGlobalTableAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.UpdateGlobalTableSettingsAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.UpdateItemAsync(tableName, key, attributeUpdates, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, ReturnValue returnValues, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.UpdateItemAsync(tableName, key, attributeUpdates, returnValues, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.UpdateItemAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.UpdateTableAsync(tableName, provisionedThroughput, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.UpdateTableAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.UpdateTableReplicaAutoScalingAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public async Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                return await _db.UpdateTimeToLiveAsync(request, cancellationToken);
            }
            catch (AmazonServiceException e)
            {
                throw PrimarSqlException.From(e);
            }
        }

        public void Dispose()
        {
            _db.Dispose();
        }
    }
}
