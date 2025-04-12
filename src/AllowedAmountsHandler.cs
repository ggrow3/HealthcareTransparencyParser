using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace HealthcareTransparencyParser
{
    /// <summary>
    /// Interface for allowed-amounts schema handler
    /// </summary>
    public interface IAllowedAmountsHandler : ISchemaHandler
    {
    }

    /// <summary>
    /// Handles processing for allowed-amounts schema
    /// </summary>
    public class AllowedAmountsHandler : IAllowedAmountsHandler
    {
        private readonly ILogger<AllowedAmountsHandler> _logger;

        public AllowedAmountsHandler(ILogger<AllowedAmountsHandler> logger)
        {
            _logger = logger;
        }

        public async Task SetupDatabaseAsync(string connectionString)
        {
            _logger.LogInformation("Creating database schema for allowed-amounts");

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            // Create tables for allowed-amounts schema if they don't exist
            var createTablesScript = @"
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transparency')
BEGIN
    EXEC('CREATE SCHEMA transparency')
END

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AllowedAmountsItems' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    CREATE TABLE transparency.AllowedAmountsItems (
        ItemId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        ReportingEntityName NVARCHAR(255) NULL,
        ReportingEntityType NVARCHAR(50) NULL,
        LastUpdatedOn DATETIME2 NULL,
        Version NVARCHAR(50) NULL,
        BillingCode NVARCHAR(50) NULL,
        BillingCodeType NVARCHAR(50) NULL,
        BillingCodeTypeVersion NVARCHAR(50) NULL,
        NegotiationArrangement NVARCHAR(50) NULL,
        Description NVARCHAR(500) NULL,
        LastUpdated DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    )
END

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AllowedAmountsProviders' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    CREATE TABLE transparency.AllowedAmountsProviders (
        Id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        ItemId UNIQUEIDENTIFIER NOT NULL,
        ProviderId NVARCHAR(100) NULL,
        NPI NVARCHAR(50) NULL,
        TIN_Type NVARCHAR(50) NULL,
        TIN_Value NVARCHAR(50) NULL,
        ServiceCode NVARCHAR(50) NULL,
        BillingClass NVARCHAR(50) NULL,
        FOREIGN KEY (ItemId) REFERENCES transparency.AllowedAmountsItems(ItemId) ON DELETE CASCADE
    )
END

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AllowedAmountsRates' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    CREATE TABLE transparency.AllowedAmountsRates (
        Id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        ItemId UNIQUEIDENTIFIER NOT NULL,
        AllowedAmount DECIMAL(18, 2) NULL,
        BilledService NVARCHAR(255) NULL,
        BillingCurrencyCode NVARCHAR(3) NULL,
        BillingCurrencyUnit NVARCHAR(20) NULL,
        ExpirationDate DATETIME2 NULL,
        ServiceCode NVARCHAR(50) NULL,
        FOREIGN KEY (ItemId) REFERENCES transparency.AllowedAmountsItems(ItemId) ON DELETE CASCADE
    )
END";

            await connection.ExecuteAsync(createTablesScript);
            _logger.LogInformation("Database schema created successfully for allowed-amounts");
        }

        public async Task ProcessStreamAsync(Stream stream, string connectionString, string lastProcessedId, int batchSize)
        {
            _logger.LogInformation($"Processing allowed-amounts stream, last processed ID: {lastProcessedId ?? "none"}");

            // Use a streaming approach to process the JSON
            using var jsonDocument = await JsonDocument.ParseAsync(stream);
            var root = jsonDocument.RootElement;

            // Process allowed amounts
            if (root.TryGetProperty("allowed_amounts", out var allowedAmountsElement) &&
                allowedAmountsElement.ValueKind == JsonValueKind.Array)
            {
                // Use lists for efficient memory usage with large collections
                var items = new List<AllowedAmountsItem>();
                var providers = new List<AllowedAmountsProvider>();
                var rates = new List<AllowedAmountsRate>();

                int totalItems = 0;
                int batchCount = 0;
                string currentLastProcessedId = lastProcessedId;

                // Enumerate allowed amounts items
                foreach (var itemElement in allowedAmountsElement.EnumerateArray())
                {
                    var data = ParseAllowedAmountsItem(itemElement);

                    string itemId = data.Item.ItemId.ToString();

                    // Skip items we've already processed
                    if (string.IsNullOrEmpty(lastProcessedId) ||
                        string.Compare(itemId, lastProcessedId, StringComparison.Ordinal) > 0)
                    {
                        items.Add(data.Item);
                        providers.AddRange(data.Providers);
                        rates.AddRange(data.Rates);

                        totalItems++;

                        // When batch size is reached, save and update state
                        if (items.Count >= batchSize)
                        {
                            // Sort items by ID for consistent processing
                            items.Sort((a, b) => a.ItemId.CompareTo(b.ItemId));

                            await SaveAllowedAmountsBulkBatchAsync(connectionString, items, providers, rates);

                            // Update last processed ID
                            currentLastProcessedId = items.Last().ItemId.ToString();
                            await UpdateProcessingStateAsync(connectionString, currentLastProcessedId);

                            batchCount++;
                            _logger.LogInformation($"Processed batch {batchCount}, items: {totalItems}, last ID: {currentLastProcessedId}");

                            // Clear the batches
                            items.Clear();
                            providers.Clear();
                            rates.Clear();
                        }
                    }
                }

                // Process any remaining items
                if (items.Count > 0)
                {
                    items.Sort((a, b) => a.ItemId.CompareTo(b.ItemId));
                    await SaveAllowedAmountsBulkBatchAsync(connectionString, items, providers, rates);

                    currentLastProcessedId = items.Last().ItemId.ToString();
                    await UpdateProcessingStateAsync(connectionString, currentLastProcessedId);

                    batchCount++;
                    _logger.LogInformation($"Processed final batch {batchCount}, total items: {totalItems}, last ID: {currentLastProcessedId}");
                }

                _logger.LogInformation($"Total allowed amounts items processed: {totalItems}");
            }
            else
            {
                _logger.LogWarning("No allowed_amounts array found in the JSON file");
            }
        }

        private class AllowedAmountsParseResult
        {
            public AllowedAmountsItem Item { get; set; }
            public List<AllowedAmountsProvider> Providers { get; set; }
            public List<AllowedAmountsRate> Rates { get; set; }
        }

        private AllowedAmountsParseResult ParseAllowedAmountsItem(JsonElement itemElement)
        {
            var result = new AllowedAmountsParseResult
            {
                Item = new AllowedAmountsItem
                {
                    ItemId = Guid.NewGuid()
                },
                Providers = new List<AllowedAmountsProvider>(),
                Rates = new List<AllowedAmountsRate>()
            };

            // Parse metadata
            if (itemElement.TryGetProperty("reporting_entity_name", out var entityNameElement))
            {
                result.Item.ReportingEntityName = entityNameElement.GetString();
            }

            if (itemElement.TryGetProperty("reporting_entity_type", out var entityTypeElement))
            {
                result.Item.ReportingEntityType = entityTypeElement.GetString();
            }

            if (itemElement.TryGetProperty("last_updated_on", out var lastUpdatedElement))
            {
                if (DateTime.TryParse(lastUpdatedElement.GetString(), out var lastUpdatedDate))
                {
                    result.Item.LastUpdatedOn = lastUpdatedDate;
                }
            }

            if (itemElement.TryGetProperty("version", out var versionElement))
            {
                result.Item.Version = versionElement.GetString();
            }

            // Parse billing code info
            if (itemElement.TryGetProperty("billing_code", out var billingCodeElement))
            {
                result.Item.BillingCode = billingCodeElement.GetString();
            }

            if (itemElement.TryGetProperty("billing_code_type", out var billingCodeTypeElement))
            {
                result.Item.BillingCodeType = billingCodeTypeElement.GetString();
            }

            if (itemElement.TryGetProperty("billing_code_type_version", out var billingCodeTypeVersionElement))
            {
                result.Item.BillingCodeTypeVersion = billingCodeTypeVersionElement.GetString();
            }

            if (itemElement.TryGetProperty("negotiation_arrangement", out var arrangementElement))
            {
                result.Item.NegotiationArrangement = arrangementElement.GetString();
            }

            if (itemElement.TryGetProperty("description", out var descriptionElement))
            {
                result.Item.Description = descriptionElement.GetString();
            }

            // Parse providers
            if (itemElement.TryGetProperty("providers", out var providersElement) &&
                providersElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var providerElement in providersElement.EnumerateArray())
                {
                    var provider = new AllowedAmountsProvider
                    {
                        Id = Guid.NewGuid(),
                        ItemId = result.Item.ItemId
                    };

                    if (providerElement.TryGetProperty("provider_references", out var referencesElement) &&
                        referencesElement.ValueKind == JsonValueKind.Array &&
                        referencesElement.GetArrayLength() > 0)
                    {
                        // Just take the first reference for simplicity
                        provider.ProviderId = referencesElement[0].GetString();
                    }

                    if (providerElement.TryGetProperty("npi", out var npiElement))
                    {
                        provider.NPI = npiElement.GetString();
                    }

                    // Parse TIN
                    if (providerElement.TryGetProperty("tin", out var tinElement) &&
                        tinElement.ValueKind == JsonValueKind.Object)
                    {
                        if (tinElement.TryGetProperty("type", out var typeElement))
                        {
                            provider.TIN_Type = typeElement.GetString();
                        }

                        if (tinElement.TryGetProperty("value", out var valueElement))
                        {
                            provider.TIN_Value = valueElement.GetString();
                        }
                    }

                    if (providerElement.TryGetProperty("service_code", out var serviceCodeElement))
                    {
                        provider.ServiceCode = serviceCodeElement.GetString();
                    }

                    if (providerElement.TryGetProperty("billing_class", out var billingClassElement))
                    {
                        provider.BillingClass = billingClassElement.GetString();
                    }

                    result.Providers.Add(provider);
                }
            }

            // Parse rates
            if (itemElement.TryGetProperty("allowed_amounts", out var ratesElement) &&
                ratesElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var rateElement in ratesElement.EnumerateArray())
                {
                    var rate = new AllowedAmountsRate
                    {
                        Id = Guid.NewGuid(),
                        ItemId = result.Item.ItemId
                    };

                    if (rateElement.TryGetProperty("allowed_amount", out var amountElement))
                    {
                        if (amountElement.TryGetDecimal(out var amount))
                        {
                            rate.AllowedAmount = amount;
                        }
                    }

                    if (rateElement.TryGetProperty("billed_service", out var billedServiceElement))
                    {
                        rate.BilledService = billedServiceElement.GetString();
                    }

                    if (rateElement.TryGetProperty("billing_currency", out var currencyElement) &&
                        currencyElement.ValueKind == JsonValueKind.Object)
                    {
                        if (currencyElement.TryGetProperty("code", out var codeElement))
                        {
                            rate.BillingCurrencyCode = codeElement.GetString();
                        }

                        if (currencyElement.TryGetProperty("unit", out var unitElement))
                        {
                            rate.BillingCurrencyUnit = unitElement.GetString();
                        }
                    }

                    if (rateElement.TryGetProperty("expiration_date", out var expirationElement))
                    {
                        if (DateTime.TryParse(expirationElement.GetString(), out var expirationDate))
                        {
                            rate.ExpirationDate = expirationDate;
                        }
                    }

                    if (rateElement.TryGetProperty("service_code", out var serviceCodeElement))
                    {
                        rate.ServiceCode = serviceCodeElement.GetString();
                    }

                    result.Rates.Add(rate);
                }
            }

            return result;
        }

        private async Task SaveAllowedAmountsBulkBatchAsync(
    string connectionString,
    List<AllowedAmountsItem> items,
    List<AllowedAmountsProvider> providers,
    List<AllowedAmountsRate> rates)
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            using var transaction = connection.BeginTransaction();

            try
            {
                // Prepare items DataTable
                DataTable itemsTable = new DataTable();
                itemsTable.Columns.Add("ItemId", typeof(Guid));
                itemsTable.Columns.Add("ReportingEntityName", typeof(string));
                itemsTable.Columns.Add("ReportingEntityType", typeof(string));
                itemsTable.Columns.Add("LastUpdatedOn", typeof(DateTime));
                itemsTable.Columns.Add("Version", typeof(string));
                itemsTable.Columns.Add("BillingCode", typeof(string));
                itemsTable.Columns.Add("BillingCodeType", typeof(string));
                itemsTable.Columns.Add("BillingCodeTypeVersion", typeof(string));
                itemsTable.Columns.Add("NegotiationArrangement", typeof(string));
                itemsTable.Columns.Add("Description", typeof(string));

                foreach (var item in items)
                {
                    DataRow row = itemsTable.NewRow();
                    row["ItemId"] = item.ItemId;
                    row["ReportingEntityName"] = item.ReportingEntityName ?? (object)DBNull.Value;
                    row["ReportingEntityType"] = item.ReportingEntityType ?? (object)DBNull.Value;
                    row["LastUpdatedOn"] = item.LastUpdatedOn ?? (object)DBNull.Value;
                    row["Version"] = item.Version ?? (object)DBNull.Value;
                    row["BillingCode"] = item.BillingCode ?? (object)DBNull.Value;
                    row["BillingCodeType"] = item.BillingCodeType ?? (object)DBNull.Value;
                    row["BillingCodeTypeVersion"] = item.BillingCodeTypeVersion ?? (object)DBNull.Value;
                    row["NegotiationArrangement"] = item.NegotiationArrangement ?? (object)DBNull.Value;
                    row["Description"] = item.Description ?? (object)DBNull.Value;
                    itemsTable.Rows.Add(row);
                }

                // Bulk insert items
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "transparency.AllowedAmountsItems";
                    bulkCopy.BulkCopyTimeout = 600; // 10 minutes
                    bulkCopy.BatchSize = 10000;

                    // Add column mappings
                    bulkCopy.ColumnMappings.Add("ItemId", "ItemId");
                    bulkCopy.ColumnMappings.Add("ReportingEntityName", "ReportingEntityName");
                    bulkCopy.ColumnMappings.Add("ReportingEntityType", "ReportingEntityType");
                    bulkCopy.ColumnMappings.Add("LastUpdatedOn", "LastUpdatedOn");
                    bulkCopy.ColumnMappings.Add("Version", "Version");
                    bulkCopy.ColumnMappings.Add("BillingCode", "BillingCode");
                    bulkCopy.ColumnMappings.Add("BillingCodeType", "BillingCodeType");
                    bulkCopy.ColumnMappings.Add("BillingCodeTypeVersion", "BillingCodeTypeVersion");
                    bulkCopy.ColumnMappings.Add("NegotiationArrangement", "NegotiationArrangement");
                    bulkCopy.ColumnMappings.Add("Description", "Description");

                    await bulkCopy.WriteToServerAsync(itemsTable);
                }

                // Bulk insert providers
                if (providers.Count > 0)
                {
                    DataTable providersTable = new DataTable();
                    providersTable.Columns.Add("Id", typeof(Guid));
                    providersTable.Columns.Add("ItemId", typeof(Guid));
                    providersTable.Columns.Add("ProviderId", typeof(string));
                    providersTable.Columns.Add("NPI", typeof(string));
                    providersTable.Columns.Add("TIN_Type", typeof(string));
                    providersTable.Columns.Add("TIN_Value", typeof(string));
                    providersTable.Columns.Add("ServiceCode", typeof(string));
                    providersTable.Columns.Add("BillingClass", typeof(string));

                    foreach (var provider in providers)
                    {
                        DataRow row = providersTable.NewRow();
                        row["Id"] = provider.Id;
                        row["ItemId"] = provider.ItemId;
                        row["ProviderId"] = provider.ProviderId ?? (object)DBNull.Value;
                        row["NPI"] = provider.NPI ?? (object)DBNull.Value;
                        row["TIN_Type"] = provider.TIN_Type ?? (object)DBNull.Value;
                        row["TIN_Value"] = provider.TIN_Value ?? (object)DBNull.Value;
                        row["ServiceCode"] = provider.ServiceCode ?? (object)DBNull.Value;
                        row["BillingClass"] = provider.BillingClass ?? (object)DBNull.Value;
                        providersTable.Rows.Add(row);
                    }

                    using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                    {
                        bulkCopy.DestinationTableName = "transparency.AllowedAmountsProviders";
                        bulkCopy.BulkCopyTimeout = 600;
                        bulkCopy.BatchSize = 10000;

                        // Add column mappings
                        bulkCopy.ColumnMappings.Add("Id", "Id");
                        bulkCopy.ColumnMappings.Add("ItemId", "ItemId");
                        bulkCopy.ColumnMappings.Add("ProviderId", "ProviderId");
                        bulkCopy.ColumnMappings.Add("NPI", "NPI");
                        bulkCopy.ColumnMappings.Add("TIN_Type", "TIN_Type");
                        bulkCopy.ColumnMappings.Add("TIN_Value", "TIN_Value");
                        bulkCopy.ColumnMappings.Add("ServiceCode", "ServiceCode");
                        bulkCopy.ColumnMappings.Add("BillingClass", "BillingClass");

                        await bulkCopy.WriteToServerAsync(providersTable);
                    }
                }

                // Bulk insert rates
                if (rates.Count > 0)
                {
                    DataTable ratesTable = new DataTable();
                    ratesTable.Columns.Add("Id", typeof(Guid));
                    ratesTable.Columns.Add("ItemId", typeof(Guid));
                    ratesTable.Columns.Add("AllowedAmount", typeof(decimal));
                    ratesTable.Columns.Add("BilledService", typeof(string));
                    ratesTable.Columns.Add("BillingCurrencyCode", typeof(string));
                    ratesTable.Columns.Add("BillingCurrencyUnit", typeof(string));
                    ratesTable.Columns.Add("ExpirationDate", typeof(DateTime));
                    ratesTable.Columns.Add("ServiceCode", typeof(string));

                    foreach (var rate in rates)
                    {
                        DataRow row = ratesTable.NewRow();
                        row["Id"] = rate.Id;
                        row["ItemId"] = rate.ItemId;
                        row["AllowedAmount"] = rate.AllowedAmount ?? (object)DBNull.Value;
                        row["BilledService"] = rate.BilledService ?? (object)DBNull.Value;
                        row["BillingCurrencyCode"] = rate.BillingCurrencyCode ?? (object)DBNull.Value;
                        row["BillingCurrencyUnit"] = rate.BillingCurrencyUnit ?? (object)DBNull.Value;
                        row["ExpirationDate"] = rate.ExpirationDate ?? (object)DBNull.Value;
                        row["ServiceCode"] = rate.ServiceCode ?? (object)DBNull.Value;
                        ratesTable.Rows.Add(row);
                    }

                    using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                    {
                        bulkCopy.DestinationTableName = "transparency.AllowedAmountsRates";
                        bulkCopy.BulkCopyTimeout = 600;
                        bulkCopy.BatchSize = 10000;

                        // Add column mappings
                        bulkCopy.ColumnMappings.Add("Id", "Id");
                        bulkCopy.ColumnMappings.Add("ItemId", "ItemId");
                        bulkCopy.ColumnMappings.Add("AllowedAmount", "AllowedAmount");
                        bulkCopy.ColumnMappings.Add("BilledService", "BilledService");
                        bulkCopy.ColumnMappings.Add("BillingCurrencyCode", "BillingCurrencyCode");
                        bulkCopy.ColumnMappings.Add("BillingCurrencyUnit", "BillingCurrencyUnit");
                        bulkCopy.ColumnMappings.Add("ExpirationDate", "ExpirationDate");
                        bulkCopy.ColumnMappings.Add("ServiceCode", "ServiceCode");

                        await bulkCopy.WriteToServerAsync(ratesTable);
                    }
                }

                await transaction.CommitAsync();
                _logger.LogInformation($"Bulk inserted {items.Count} items, {providers.Count} providers, and {rates.Count} rates");
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.LogError(ex, "Error bulk saving allowed amounts batch");
                throw;
            }
        }

        private async Task SaveAllowedAmountsBatchAsync(
            string connectionString,
            List<AllowedAmountsItem> items,
            List<AllowedAmountsProvider> providers,
            List<AllowedAmountsRate> rates)
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            using var transaction = connection.BeginTransaction();

            try
            {
                // Insert items
                var itemInsertQuery = @"
INSERT INTO transparency.AllowedAmountsItems (
    ItemId, ReportingEntityName, ReportingEntityType, LastUpdatedOn, Version,
    BillingCode, BillingCodeType, BillingCodeTypeVersion,
    NegotiationArrangement, Description)
VALUES (
    @ItemId, @ReportingEntityName, @ReportingEntityType, @LastUpdatedOn, @Version,
    @BillingCode, @BillingCodeType, @BillingCodeTypeVersion,
    @NegotiationArrangement, @Description)";

                foreach (var item in items)
                {
                    await connection.ExecuteAsync(itemInsertQuery, item, transaction);
                }

                // Insert providers
                if (providers.Count > 0)
                {
                    var providerInsertQuery = @"
INSERT INTO transparency.AllowedAmountsProviders (
    Id, ItemId, ProviderId, NPI, TIN_Type, TIN_Value, ServiceCode, BillingClass)
VALUES (
    @Id, @ItemId, @ProviderId, @NPI, @TIN_Type, @TIN_Value, @ServiceCode, @BillingClass)";

                    await connection.ExecuteAsync(providerInsertQuery, providers, transaction);
                }

                // Insert rates
                if (rates.Count > 0)
                {
                    var rateInsertQuery = @"
INSERT INTO transparency.AllowedAmountsRates (
    Id, ItemId, AllowedAmount, BilledService, BillingCurrencyCode, 
    BillingCurrencyUnit, ExpirationDate, ServiceCode)
VALUES (
    @Id, @ItemId, @AllowedAmount, @BilledService, @BillingCurrencyCode,
    @BillingCurrencyUnit, @ExpirationDate, @ServiceCode)";

                    await connection.ExecuteAsync(rateInsertQuery, rates, transaction);
                }

                await transaction.CommitAsync();
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.LogError(ex, "Error saving allowed amounts batch");
                throw;
            }
        }

        private async Task UpdateProcessingStateAsync(string connectionString, string lastProcessedId)
        {
            using var connection = new SqlConnection(connectionString);

            var updateQuery = @"
UPDATE transparency.ProcessingState 
SET LastProcessedId = @LastProcessedId
WHERE SchemaType = 'allowed-amounts'";

            await connection.ExecuteAsync(updateQuery, new { LastProcessedId = lastProcessedId });
        }
    }

    /// <summary>
    /// Represents an allowed amounts item
    /// </summary>
    public class AllowedAmountsItem
    {
        public Guid ItemId { get; set; }
        public string ReportingEntityName { get; set; }
        public string ReportingEntityType { get; set; }
        public DateTime? LastUpdatedOn { get; set; }
        public string Version { get; set; }
        public string BillingCode { get; set; }
        public string BillingCodeType { get; set; }
        public string BillingCodeTypeVersion { get; set; }
        public string NegotiationArrangement { get; set; }
        public string Description { get; set; }
    }

    /// <summary>
    /// Represents an allowed amounts provider
    /// </summary>
    public class AllowedAmountsProvider
    {
        public Guid Id { get; set; }
        public Guid ItemId { get; set; }
        public string ProviderId { get; set; }
        public string NPI { get; set; }
        public string TIN_Type { get; set; }
        public string TIN_Value { get; set; }
        public string ServiceCode { get; set; }
        public string BillingClass { get; set; }
    }

    /// <summary>
    /// Represents an allowed amounts rate
    /// </summary>
    public class AllowedAmountsRate
    {
        public Guid Id { get; set; }
        public Guid ItemId { get; set; }
        public decimal? AllowedAmount { get; set; }
        public string BilledService { get; set; }
        public string BillingCurrencyCode { get; set; }
        public string BillingCurrencyUnit { get; set; }
        public DateTime? ExpirationDate { get; set; }
        public string ServiceCode { get; set; }
    }
}