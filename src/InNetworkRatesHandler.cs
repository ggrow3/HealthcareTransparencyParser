using System;
using System.Collections.Generic;
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
    /// Interface for in-network-rates schema handler
    /// </summary>
    public interface IInNetworkRatesHandler : ISchemaHandler
    {
    }

    /// <summary>
    /// Handles processing for in-network-rates schema
    /// </summary>
    public class InNetworkRatesHandler : IInNetworkRatesHandler
    {
        private readonly ILogger<InNetworkRatesHandler> _logger;

        public InNetworkRatesHandler(ILogger<InNetworkRatesHandler> logger)
        {
            _logger = logger;
        }

        public async Task SetupDatabaseAsync(string connectionString)
        {
            _logger.LogInformation("Creating database schema for in-network-rates");

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            // Create tables for in-network-rates schema if they don't exist
            var createTablesScript = @"
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transparency')
BEGIN
    EXEC('CREATE SCHEMA transparency')
END

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'InNetworkRatesItems' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    CREATE TABLE transparency.InNetworkRatesItems (
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
        BundledCodes NVARCHAR(MAX) NULL,
        LastUpdated DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    )
END

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'InNetworkProviderGroups' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    CREATE TABLE transparency.InNetworkProviderGroups (
        GroupId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        ItemId UNIQUEIDENTIFIER NOT NULL,
        ProviderGroupId NVARCHAR(100) NULL,
        FOREIGN KEY (ItemId) REFERENCES transparency.InNetworkRatesItems(ItemId) ON DELETE CASCADE
    )
END

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'InNetworkProviderReferences' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    CREATE TABLE transparency.InNetworkProviderReferences (
        Id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        GroupId UNIQUEIDENTIFIER NOT NULL,
        ProviderReference NVARCHAR(100) NULL,
        FOREIGN KEY (GroupId) REFERENCES transparency.InNetworkProviderGroups(GroupId) ON DELETE CASCADE
    )
END

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'InNetworkRates' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    CREATE TABLE transparency.InNetworkRates (
        Id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        GroupId UNIQUEIDENTIFIER NOT NULL,
        NegotiatedType NVARCHAR(50) NULL,
        NegotiatedRate DECIMAL(18, 2) NULL,
        ExpirationDate DATETIME2 NULL,
        ServiceCode NVARCHAR(50) NULL,
        BillingCurrencyCode NVARCHAR(3) NULL,
        BillingCurrencyUnit NVARCHAR(20) NULL,
        AdditionalInfo NVARCHAR(MAX) NULL,
        FOREIGN KEY (GroupId) REFERENCES transparency.InNetworkProviderGroups(GroupId) ON DELETE CASCADE
    )
END";

            await connection.ExecuteAsync(createTablesScript);
            _logger.LogInformation("Database schema created successfully for in-network-rates");
        }

        public async Task ProcessStreamAsync(Stream stream, string connectionString, string lastProcessedId, int batchSize)
        {
            _logger.LogInformation($"Processing in-network-rates stream, last processed ID: {lastProcessedId ?? "none"}");

            // Use a streaming approach to process the JSON
            using var jsonDocument = await JsonDocument.ParseAsync(stream);
            var root = jsonDocument.RootElement;

            // Process in-network rates
            if (root.TryGetProperty("in_network", out var inNetworkElement) &&
                inNetworkElement.ValueKind == JsonValueKind.Array)
            {
                // Use lists for efficient memory usage with large collections
                var items = new List<InNetworkRatesItem>();
                var providerGroups = new List<InNetworkProviderGroup>();
                var providerReferences = new List<InNetworkProviderReference>();
                var rates = new List<InNetworkRate>();

                int totalItems = 0;
                int batchCount = 0;
                string currentLastProcessedId = lastProcessedId;

                // Enumerate in-network rates items
                foreach (var itemElement in inNetworkElement.EnumerateArray())
                {
                    var data = ParseInNetworkRatesItem(itemElement);

                    string itemId = data.Item.ItemId.ToString();

                    // Skip items we've already processed
                    if (string.IsNullOrEmpty(lastProcessedId) ||
                        string.Compare(itemId, lastProcessedId, StringComparison.Ordinal) > 0)
                    {
                        items.Add(data.Item);
                        providerGroups.AddRange(data.ProviderGroups);
                        providerReferences.AddRange(data.ProviderReferences);
                        rates.AddRange(data.Rates);

                        totalItems++;

                        // When batch size is reached, save and update state
                        if (items.Count >= batchSize)
                        {
                            // Sort items by ID for consistent processing
                            items.Sort((a, b) => a.ItemId.CompareTo(b.ItemId));

                            await SaveInNetworkRatesBatchAsync(connectionString, items, providerGroups, providerReferences, rates);

                            // Update last processed ID
                            currentLastProcessedId = items.Last().ItemId.ToString();
                            await UpdateProcessingStateAsync(connectionString, currentLastProcessedId);

                            batchCount++;
                            _logger.LogInformation($"Processed batch {batchCount}, items: {totalItems}, last ID: {currentLastProcessedId}");

                            // Clear the batches
                            items.Clear();
                            providerGroups.Clear();
                            providerReferences.Clear();
                            rates.Clear();
                        }
                    }
                }

                // Process any remaining items
                if (items.Count > 0)
                {
                    items.Sort((a, b) => a.ItemId.CompareTo(b.ItemId));
                    await SaveInNetworkRatesBatchAsync(connectionString, items, providerGroups, providerReferences, rates);

                    currentLastProcessedId = items.Last().ItemId.ToString();
                    await UpdateProcessingStateAsync(connectionString, currentLastProcessedId);

                    batchCount++;
                    _logger.LogInformation($"Processed final batch {batchCount}, total items: {totalItems}, last ID: {currentLastProcessedId}");
                }

                _logger.LogInformation($"Total in-network rates items processed: {totalItems}");
            }
            else
            {
                _logger.LogWarning("No in_network array found in the JSON file");
            }
        }

        private class InNetworkRatesParseResult
        {
            public InNetworkRatesItem Item { get; set; }
            public List<InNetworkProviderGroup> ProviderGroups { get; set; }
            public List<InNetworkProviderReference> ProviderReferences { get; set; }
            public List<InNetworkRate> Rates { get; set; }
        }

        private InNetworkRatesParseResult ParseInNetworkRatesItem(JsonElement itemElement)
        {
            var result = new InNetworkRatesParseResult
            {
                Item = new InNetworkRatesItem
                {
                    ItemId = Guid.NewGuid()
                },
                ProviderGroups = new List<InNetworkProviderGroup>(),
                ProviderReferences = new List<InNetworkProviderReference>(),
                Rates = new List<InNetworkRate>()
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

            // Parse bundled codes
            if (itemElement.TryGetProperty("bundled_codes", out var bundledCodesElement))
            {
                // Serialize bundled codes as JSON string to store in the database
                result.Item.BundledCodes = bundledCodesElement.GetRawText();
            }

            // Parse negotiated prices
            if (itemElement.TryGetProperty("negotiated_prices", out var pricesElement) &&
                pricesElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var priceElement in pricesElement.EnumerateArray())
                {
                    // Create a provider group for each negotiated price
                    var providerGroup = new InNetworkProviderGroup
                    {
                        GroupId = Guid.NewGuid(),
                        ItemId = result.Item.ItemId
                    };

                    // Process provider group
                    if (priceElement.TryGetProperty("provider_group_id", out var groupIdElement))
                    {
                        providerGroup.ProviderGroupId = groupIdElement.GetString();
                    }

                    result.ProviderGroups.Add(providerGroup);

                    // Process provider references
                    if (priceElement.TryGetProperty("provider_references", out var referencesElement) &&
                        referencesElement.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var referenceElement in referencesElement.EnumerateArray())
                        {
                            var reference = new InNetworkProviderReference
                            {
                                Id = Guid.NewGuid(),
                                GroupId = providerGroup.GroupId,
                                ProviderReference = referenceElement.GetString()
                            };

                            result.ProviderReferences.Add(reference);
                        }
                    }

                    // Process negotiated rates
                    if (priceElement.TryGetProperty("negotiated_rates", out var ratesElement) &&
                        ratesElement.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var rateElement in ratesElement.EnumerateArray())
                        {
                            var rate = new InNetworkRate
                            {
                                Id = Guid.NewGuid(),
                                GroupId = providerGroup.GroupId
                            };

                            if (rateElement.TryGetProperty("negotiated_type", out var typeElement))
                            {
                                rate.NegotiatedType = typeElement.GetString();
                            }

                            if (rateElement.TryGetProperty("negotiated_rate", out var rateValueElement))
                            {
                                if (rateValueElement.TryGetDecimal(out var rateValue))
                                {
                                    rate.NegotiatedRate = rateValue;
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

                            // Store additional info as JSON string
                            if (rateElement.TryGetProperty("additional_information", out var additionalInfoElement))
                            {
                                rate.AdditionalInfo = additionalInfoElement.GetRawText();
                            }

                            result.Rates.Add(rate);
                        }
                    }
                }
            }

            return result;
        }

        private async Task SaveInNetworkRatesBatchAsync(
            string connectionString,
            List<InNetworkRatesItem> items,
            List<InNetworkProviderGroup> providerGroups,
            List<InNetworkProviderReference> providerReferences,
            List<InNetworkRate> rates)
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            using var transaction = connection.BeginTransaction();

            try
            {
                // Insert items
                var itemInsertQuery = @"
INSERT INTO transparency.InNetworkRatesItems (
    ItemId, ReportingEntityName, ReportingEntityType, LastUpdatedOn, Version,
    BillingCode, BillingCodeType, BillingCodeTypeVersion,
    NegotiationArrangement, Description, BundledCodes)
VALUES (
    @ItemId, @ReportingEntityName, @ReportingEntityType, @LastUpdatedOn, @Version,
    @BillingCode, @BillingCodeType, @BillingCodeTypeVersion,
    @NegotiationArrangement, @Description, @BundledCodes)";

                foreach (var item in items)
                {
                    await connection.ExecuteAsync(itemInsertQuery, item, transaction);
                }

                // Insert provider groups
                if (providerGroups.Count > 0)
                {
                    var groupInsertQuery = @"
INSERT INTO transparency.InNetworkProviderGroups (
    GroupId, ItemId, ProviderGroupId)
VALUES (
    @GroupId, @ItemId, @ProviderGroupId)";

                    await connection.ExecuteAsync(groupInsertQuery, providerGroups, transaction);
                }

                // Insert provider references
                if (providerReferences.Count > 0)
                {
                    var referenceInsertQuery = @"
INSERT INTO transparency.InNetworkProviderReferences (
    Id, GroupId, ProviderReference)
VALUES (
    @Id, @GroupId, @ProviderReference)";

                    await connection.ExecuteAsync(referenceInsertQuery, providerReferences, transaction);
                }

                // Insert rates
                if (rates.Count > 0)
                {
                    var rateInsertQuery = @"
INSERT INTO transparency.InNetworkRates (
    Id, GroupId, NegotiatedType, NegotiatedRate, ExpirationDate, 
    ServiceCode, BillingCurrencyCode, BillingCurrencyUnit, AdditionalInfo)
VALUES (
    @Id, @GroupId, @NegotiatedType, @NegotiatedRate, @ExpirationDate,
    @ServiceCode, @BillingCurrencyCode, @BillingCurrencyUnit, @AdditionalInfo)";

                    await connection.ExecuteAsync(rateInsertQuery, rates, transaction);
                }

                await transaction.CommitAsync();
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.LogError(ex, "Error saving in-network rates batch");
                throw;
            }
        }

        private async Task UpdateProcessingStateAsync(string connectionString, string lastProcessedId)
        {
            using var connection = new SqlConnection(connectionString);

            var updateQuery = @"
UPDATE transparency.ProcessingState 
SET LastProcessedId = @LastProcessedId
WHERE SchemaType = 'in-network-rates'";

            await connection.ExecuteAsync(updateQuery, new { LastProcessedId = lastProcessedId });
        }
    }

    /// <summary>
    /// Represents an in-network rates item
    /// </summary>
    public class InNetworkRatesItem
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
        public string BundledCodes { get; set; }
    }

    /// <summary>
    /// Represents an in-network provider group
    /// </summary>
    public class InNetworkProviderGroup
    {
        public Guid GroupId { get; set; }
        public Guid ItemId { get; set; }
        public string ProviderGroupId { get; set; }
    }

    /// <summary>
    /// Represents an in-network provider reference
    /// </summary>
    public class InNetworkProviderReference
    {
        public Guid Id { get; set; }
        public Guid GroupId { get; set; }
        public string ProviderReference { get; set; }
    }

    /// <summary>
    /// Represents an in-network rate
    /// </summary>
    public class InNetworkRate
    {
        public Guid Id { get; set; }
        public Guid GroupId { get; set; }
        public string NegotiatedType { get; set; }
        public decimal? NegotiatedRate { get; set; }
        public DateTime? ExpirationDate { get; set; }
        public string ServiceCode { get; set; }
        public string BillingCurrencyCode { get; set; }
        public string BillingCurrencyUnit { get; set; }
        public string AdditionalInfo { get; set; }
    }
}