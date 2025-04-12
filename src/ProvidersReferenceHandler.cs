using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Data;

namespace HealthcareTransparencyParser
{
    /// <summary>
    /// Handles processing for providers-reference schema
    /// </summary>
    public class ProvidersReferenceHandler : IProvidersReferenceHandler
    {
        private readonly ILogger<ProvidersReferenceHandler> _logger;

        public ProvidersReferenceHandler(ILogger<ProvidersReferenceHandler> logger)
        {
            _logger = logger;
        }

        public async Task SetupDatabaseAsync(string connectionString)
        {
            _logger.LogInformation("Creating database schema for providers-reference");

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            // Create tables for providers-reference schema if they don't exist
            var createTablesScript = @"
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transparency')
BEGIN
    EXEC('CREATE SCHEMA transparency')
END

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Providers' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    CREATE TABLE transparency.Providers (
        ProviderId NVARCHAR(100) PRIMARY KEY,
        NPI NVARCHAR(50) NULL,
        TIN_Type NVARCHAR(50) NULL,
        TIN_Value NVARCHAR(50) NULL,
        Entity_Type NVARCHAR(50) NULL,
        Organization_Name NVARCHAR(255) NULL,
        PrimaryFirstName NVARCHAR(100) NULL,
        PrimaryMiddleName NVARCHAR(100) NULL,
        PrimaryLastName NVARCHAR(100) NULL,
        PrimarySuffix NVARCHAR(20) NULL,
        LastUpdated DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    )
END

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ProviderAddresses' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    CREATE TABLE transparency.ProviderAddresses (
        AddressId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        ProviderId NVARCHAR(100) NOT NULL,
        AddressType NVARCHAR(50) NULL,
        Address1 NVARCHAR(255) NULL,
        Address2 NVARCHAR(255) NULL,
        City NVARCHAR(100) NULL,
        State NVARCHAR(50) NULL,
        ZipCode NVARCHAR(20) NULL,
        FOREIGN KEY (ProviderId) REFERENCES transparency.Providers(ProviderId)
    )
END

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ProcessingState' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    CREATE TABLE transparency.ProcessingState (
        Id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        Url NVARCHAR(1000) NOT NULL,
        SchemaType NVARCHAR(50) NOT NULL,
        LastProcessedId NVARCHAR(255) NULL,
        IsCompleted BIT NOT NULL DEFAULT 0,
        StartedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        CompletedAt DATETIME2 NULL,
        CONSTRAINT UQ_ProcessingState_Url_SchemaType UNIQUE (Url, SchemaType)
    )
END";

            await connection.ExecuteAsync(createTablesScript);
            _logger.LogInformation("Database schema created successfully");
        }

        public async Task ProcessStreamAsync(Stream stream, string connectionString, string lastProcessedId, int batchSize)
        {
            _logger.LogInformation($"Processing providers-reference stream, last processed ID: {lastProcessedId ?? "none"}");

            // Use a streaming approach to process the JSON
            using var jsonDocument = await JsonDocument.ParseAsync(stream);
            var root = jsonDocument.RootElement;

            // Process providers
            if (root.TryGetProperty("providers", out var providersElement) &&
                providersElement.ValueKind == JsonValueKind.Array)
            {
                // Use List for efficient memory usage with large collections
                var providers = new List<Provider>();
                var addresses = new List<ProviderAddress>();

                int totalProviders = 0;
                int batchCount = 0;
                string currentLastProcessedId = lastProcessedId;

                // Enumerate providers
                foreach (var providerElement in providersElement.EnumerateArray())
                {
                    var provider = ParseProvider(providerElement);

                    // Skip providers we've already processed
                    if (provider.ProviderId != null &&
                        (string.IsNullOrEmpty(lastProcessedId) ||
                         string.Compare(provider.ProviderId, lastProcessedId, StringComparison.Ordinal) > 0))
                    {
                        providers.Add(provider);

                        // Add addresses
                        if (provider.Addresses != null)
                        {
                            foreach (var address in provider.Addresses)
                            {
                                address.ProviderId = provider.ProviderId;
                                addresses.Add(address);
                            }
                        }

                        totalProviders++;

                        // When batch size is reached, save and update state
                        if (providers.Count >= batchSize)
                        {
                            // Sort providers by ID for consistent processing
                            providers.Sort((a, b) => string.Compare(a.ProviderId, b.ProviderId, StringComparison.Ordinal));

                            await SaveProvidersBulkBatchAsync(connectionString, providers, addresses);

                            // Update last processed ID
                            currentLastProcessedId = providers.Last().ProviderId;
                            await UpdateProcessingStateAsync(connectionString, currentLastProcessedId);

                            batchCount++;
                            _logger.LogInformation($"Processed batch {batchCount}, providers: {totalProviders}, last ID: {currentLastProcessedId}");

                            // Clear the batches
                            providers.Clear();
                            addresses.Clear();
                        }
                    }
                }

                // Process any remaining providers
                if (providers.Count > 0)
                {
                    providers.Sort((a, b) => string.Compare(a.ProviderId, b.ProviderId, StringComparison.Ordinal));
                    await SaveProvidersBulkBatchAsync(connectionString, providers, addresses);

                    currentLastProcessedId = providers.Last().ProviderId;
                    await UpdateProcessingStateAsync(connectionString, currentLastProcessedId);

                    batchCount++;
                    _logger.LogInformation($"Processed final batch {batchCount}, total providers: {totalProviders}, last ID: {currentLastProcessedId}");
                }

                _logger.LogInformation($"Total providers processed: {totalProviders}");
            }
            else
            {
                _logger.LogWarning("No providers array found in the JSON file");
            }
        }

        private Provider ParseProvider(JsonElement providerElement)
        {
            var provider = new Provider();

            if (providerElement.TryGetProperty("provider_id", out var idElement))
            {
                provider.ProviderId = idElement.GetString();
            }

            if (providerElement.TryGetProperty("npi", out var npiElement))
            {
                provider.NPI = npiElement.GetString();
            }

            if (providerElement.TryGetProperty("entity_type", out var entityTypeElement))
            {
                provider.Entity_Type = entityTypeElement.GetString();
            }

            // Parse TIN object
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

            // Parse name fields
            if (providerElement.TryGetProperty("name", out var nameElement))
            {
                provider.Organization_Name = nameElement.GetString();
            }

            if (providerElement.TryGetProperty("first_name", out var firstNameElement))
            {
                provider.PrimaryFirstName = firstNameElement.GetString();
            }

            if (providerElement.TryGetProperty("middle_name", out var middleNameElement))
            {
                provider.PrimaryMiddleName = middleNameElement.GetString();
            }

            if (providerElement.TryGetProperty("last_name", out var lastNameElement))
            {
                provider.PrimaryLastName = lastNameElement.GetString();
            }

            if (providerElement.TryGetProperty("suffix", out var suffixElement))
            {
                provider.PrimarySuffix = suffixElement.GetString();
            }

            // Parse addresses
            provider.Addresses = new List<ProviderAddress>();
            if (providerElement.TryGetProperty("addresses", out var addressesElement) &&
                addressesElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var addressElement in addressesElement.EnumerateArray())
                {
                    var address = new ProviderAddress
                    {
                        AddressId = Guid.NewGuid()
                    };

                    if (addressElement.TryGetProperty("address_type", out var typeElement))
                    {
                        address.AddressType = typeElement.GetString();
                    }

                    if (addressElement.TryGetProperty("address_1", out var address1Element))
                    {
                        address.Address1 = address1Element.GetString();
                    }

                    if (addressElement.TryGetProperty("address_2", out var address2Element))
                    {
                        address.Address2 = address2Element.GetString();
                    }

                    if (addressElement.TryGetProperty("city", out var cityElement))
                    {
                        address.City = cityElement.GetString();
                    }

                    if (addressElement.TryGetProperty("state", out var stateElement))
                    {
                        address.State = stateElement.GetString();
                    }

                    if (addressElement.TryGetProperty("zip_code", out var zipElement))
                    {
                        address.ZipCode = zipElement.GetString();
                    }

                    provider.Addresses.Add(address);
                }
            }

            return provider;
        }

        private async Task SaveProvidersBulkBatchAsync(string connectionString, List<Provider> providers, List<ProviderAddress> addresses)
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            using var transaction = connection.BeginTransaction();

            try
            {
                // Convert providers to DataTable for bulk insert
                DataTable providersTable = new DataTable();
                providersTable.Columns.Add("ProviderId", typeof(string));
                providersTable.Columns.Add("NPI", typeof(string));
                providersTable.Columns.Add("TIN_Type", typeof(string));
                providersTable.Columns.Add("TIN_Value", typeof(string));
                providersTable.Columns.Add("Entity_Type", typeof(string));
                providersTable.Columns.Add("Organization_Name", typeof(string));
                providersTable.Columns.Add("PrimaryFirstName", typeof(string));
                providersTable.Columns.Add("PrimaryMiddleName", typeof(string));
                providersTable.Columns.Add("PrimaryLastName", typeof(string));
                providersTable.Columns.Add("PrimarySuffix", typeof(string));
                providersTable.Columns.Add("LastUpdated", typeof(DateTime));

                // Handle duplicate providers using a dictionary for faster lookups
                Dictionary<string, Provider> uniqueProviders = new Dictionary<string, Provider>();
                foreach (var provider in providers)
                {
                    // Use last occurrence of each provider ID
                    uniqueProviders[provider.ProviderId] = provider;
                }

                foreach (var provider in uniqueProviders.Values)
                {
                    DataRow row = providersTable.NewRow();
                    row["ProviderId"] = provider.ProviderId;
                    row["NPI"] = provider.NPI ?? (object)DBNull.Value;
                    row["TIN_Type"] = provider.TIN_Type ?? (object)DBNull.Value;
                    row["TIN_Value"] = provider.TIN_Value ?? (object)DBNull.Value;
                    row["Entity_Type"] = provider.Entity_Type ?? (object)DBNull.Value;
                    row["Organization_Name"] = provider.Organization_Name ?? (object)DBNull.Value;
                    row["PrimaryFirstName"] = provider.PrimaryFirstName ?? (object)DBNull.Value;
                    row["PrimaryMiddleName"] = provider.PrimaryMiddleName ?? (object)DBNull.Value;
                    row["PrimaryLastName"] = provider.PrimaryLastName ?? (object)DBNull.Value;
                    row["PrimarySuffix"] = provider.PrimarySuffix ?? (object)DBNull.Value;
                    row["LastUpdated"] = DateTime.UtcNow;
                    providersTable.Rows.Add(row);
                }

                // Use MERGE to handle upserts efficiently
                // First create a temporary table for the bulk insert
                await connection.ExecuteAsync(@"
            IF OBJECT_ID('tempdb..#TempProviders') IS NOT NULL
                DROP TABLE #TempProviders
            
            CREATE TABLE #TempProviders (
                ProviderId NVARCHAR(100) PRIMARY KEY,
                NPI NVARCHAR(50) NULL,
                TIN_Type NVARCHAR(50) NULL,
                TIN_Value NVARCHAR(50) NULL,
                Entity_Type NVARCHAR(50) NULL,
                Organization_Name NVARCHAR(255) NULL,
                PrimaryFirstName NVARCHAR(100) NULL,
                PrimaryMiddleName NVARCHAR(100) NULL,
                PrimaryLastName NVARCHAR(100) NULL,
                PrimarySuffix NVARCHAR(20) NULL,
                LastUpdated DATETIME2 NOT NULL
            )", transaction: transaction);

                // Bulk insert to temp table
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "#TempProviders";
                    bulkCopy.BulkCopyTimeout = 600; // 10 minutes
                    bulkCopy.BatchSize = 10000;

                    // Add column mappings
                    bulkCopy.ColumnMappings.Add("ProviderId", "ProviderId");
                    bulkCopy.ColumnMappings.Add("NPI", "NPI");
                    bulkCopy.ColumnMappings.Add("TIN_Type", "TIN_Type");
                    bulkCopy.ColumnMappings.Add("TIN_Value", "TIN_Value");
                    bulkCopy.ColumnMappings.Add("Entity_Type", "Entity_Type");
                    bulkCopy.ColumnMappings.Add("Organization_Name", "Organization_Name");
                    bulkCopy.ColumnMappings.Add("PrimaryFirstName", "PrimaryFirstName");
                    bulkCopy.ColumnMappings.Add("PrimaryMiddleName", "PrimaryMiddleName");
                    bulkCopy.ColumnMappings.Add("PrimaryLastName", "PrimaryLastName");
                    bulkCopy.ColumnMappings.Add("PrimarySuffix", "PrimarySuffix");
                    bulkCopy.ColumnMappings.Add("LastUpdated", "LastUpdated");

                    await bulkCopy.WriteToServerAsync(providersTable);
                }

                // MERGE from temp table to real table
                await connection.ExecuteAsync(@"
            MERGE transparency.Providers AS target
            USING #TempProviders AS source
            ON target.ProviderId = source.ProviderId
            WHEN MATCHED THEN
                UPDATE SET 
                    NPI = source.NPI,
                    TIN_Type = source.TIN_Type,
                    TIN_Value = source.TIN_Value,
                    Entity_Type = source.Entity_Type,
                    Organization_Name = source.Organization_Name,
                    PrimaryFirstName = source.PrimaryFirstName,
                    PrimaryMiddleName = source.PrimaryMiddleName,
                    PrimaryLastName = source.PrimaryLastName,
                    PrimarySuffix = source.PrimarySuffix,
                    LastUpdated = source.LastUpdated
            WHEN NOT MATCHED THEN
                INSERT (ProviderId, NPI, TIN_Type, TIN_Value, Entity_Type, 
                        Organization_Name, PrimaryFirstName, PrimaryMiddleName, 
                        PrimaryLastName, PrimarySuffix, LastUpdated)
                VALUES (source.ProviderId, source.NPI, source.TIN_Type, source.TIN_Value, 
                        source.Entity_Type, source.Organization_Name, source.PrimaryFirstName, 
                        source.PrimaryMiddleName, source.PrimaryLastName, source.PrimarySuffix,
                        source.LastUpdated);", transaction: transaction);

                // Handle addresses using bulk copy as well
                if (addresses.Count > 0)
                {
                    // Get provider IDs for deletion
                    var providerIds = uniqueProviders.Keys.ToArray();

                    // Delete existing addresses
                    await connection.ExecuteAsync(
                        "DELETE FROM transparency.ProviderAddresses WHERE ProviderId IN @ProviderIds",
                        new { ProviderIds = providerIds },
                        transaction);

                    // Prepare address DataTable
                    DataTable addressesTable = new DataTable();
                    addressesTable.Columns.Add("AddressId", typeof(Guid));
                    addressesTable.Columns.Add("ProviderId", typeof(string));
                    addressesTable.Columns.Add("AddressType", typeof(string));
                    addressesTable.Columns.Add("Address1", typeof(string));
                    addressesTable.Columns.Add("Address2", typeof(string));
                    addressesTable.Columns.Add("City", typeof(string));
                    addressesTable.Columns.Add("State", typeof(string));
                    addressesTable.Columns.Add("ZipCode", typeof(string));

                    foreach (var address in addresses)
                    {
                        if (uniqueProviders.ContainsKey(address.ProviderId))
                        {
                            DataRow row = addressesTable.NewRow();
                            row["AddressId"] = address.AddressId;
                            row["ProviderId"] = address.ProviderId;
                            row["AddressType"] = address.AddressType ?? (object)DBNull.Value;
                            row["Address1"] = address.Address1 ?? (object)DBNull.Value;
                            row["Address2"] = address.Address2 ?? (object)DBNull.Value;
                            row["City"] = address.City ?? (object)DBNull.Value;
                            row["State"] = address.State ?? (object)DBNull.Value;
                            row["ZipCode"] = address.ZipCode ?? (object)DBNull.Value;
                            addressesTable.Rows.Add(row);
                        }
                    }

                    // Bulk insert addresses
                    using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                    {
                        bulkCopy.DestinationTableName = "transparency.ProviderAddresses";
                        bulkCopy.BulkCopyTimeout = 600; // 10 minutes
                        bulkCopy.BatchSize = 10000;

                        // Add column mappings
                        bulkCopy.ColumnMappings.Add("AddressId", "AddressId");
                        bulkCopy.ColumnMappings.Add("ProviderId", "ProviderId");
                        bulkCopy.ColumnMappings.Add("AddressType", "AddressType");
                        bulkCopy.ColumnMappings.Add("Address1", "Address1");
                        bulkCopy.ColumnMappings.Add("Address2", "Address2");
                        bulkCopy.ColumnMappings.Add("City", "City");
                        bulkCopy.ColumnMappings.Add("State", "State");
                        bulkCopy.ColumnMappings.Add("ZipCode", "ZipCode");

                        await bulkCopy.WriteToServerAsync(addressesTable);
                    }
                }

                await transaction.CommitAsync();
                _logger.LogInformation($"Bulk inserted {uniqueProviders.Count} providers and {addresses.Count} addresses");
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.LogError(ex, "Error bulk saving providers batch");
                throw;
            }
        }

        private async Task SaveProvidersBatchAsync(string connectionString, List<Provider> providers, List<ProviderAddress> addresses)
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            using var transaction = connection.BeginTransaction();

            try
            {
                // Use batch insert approach with Dapper

                // Providers - merge to handle duplicates
                var providerMergeQuery = @"
MERGE transparency.Providers AS target
USING (SELECT @ProviderId, @NPI, @TIN_Type, @TIN_Value, @Entity_Type, 
              @Organization_Name, @PrimaryFirstName, @PrimaryMiddleName, 
              @PrimaryLastName, @PrimarySuffix) 
    AS source(ProviderId, NPI, TIN_Type, TIN_Value, Entity_Type, 
              Organization_Name, PrimaryFirstName, PrimaryMiddleName, 
              PrimaryLastName, PrimarySuffix)
ON target.ProviderId = source.ProviderId
WHEN MATCHED THEN
    UPDATE SET 
        NPI = source.NPI,
        TIN_Type = source.TIN_Type,
        TIN_Value = source.TIN_Value,
        Entity_Type = source.Entity_Type,
        Organization_Name = source.Organization_Name,
        PrimaryFirstName = source.PrimaryFirstName,
        PrimaryMiddleName = source.PrimaryMiddleName,
        PrimaryLastName = source.PrimaryLastName,
        PrimarySuffix = source.PrimarySuffix,
        LastUpdated = GETUTCDATE()
WHEN NOT MATCHED THEN
    INSERT (ProviderId, NPI, TIN_Type, TIN_Value, Entity_Type, 
            Organization_Name, PrimaryFirstName, PrimaryMiddleName, 
            PrimaryLastName, PrimarySuffix)
    VALUES (source.ProviderId, source.NPI, source.TIN_Type, source.TIN_Value, 
            source.Entity_Type, source.Organization_Name, source.PrimaryFirstName, 
            source.PrimaryMiddleName, source.PrimaryLastName, source.PrimarySuffix);";

                foreach (var provider in providers)
                {
                    await connection.ExecuteAsync(providerMergeQuery, provider, transaction);
                }

                // Delete existing addresses for these providers
                if (addresses.Count > 0)
                {
                    var providerIds = providers.Select(p => p.ProviderId).ToArray();
                    await connection.ExecuteAsync(
                        "DELETE FROM transparency.ProviderAddresses WHERE ProviderId IN @ProviderIds",
                        new { ProviderIds = providerIds },
                        transaction);

                    // Insert new addresses
                    var addressInsertQuery = @"
INSERT INTO transparency.ProviderAddresses
    (AddressId, ProviderId, AddressType, Address1, Address2, City, State, ZipCode)
VALUES
    (@AddressId, @ProviderId, @AddressType, @Address1, @Address2, @City, @State, @ZipCode)";

                    await connection.ExecuteAsync(addressInsertQuery, addresses, transaction);
                }

                await transaction.CommitAsync();
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.LogError(ex, "Error saving providers batch");
                throw;
            }
        }

        private async Task UpdateProcessingStateAsync(string connectionString, string lastProcessedId)
        {
            using var connection = new SqlConnection(connectionString);

            var updateQuery = @"
UPDATE transparency.ProcessingState 
SET LastProcessedId = @LastProcessedId
WHERE SchemaType = 'providers-reference'";

            await connection.ExecuteAsync(updateQuery, new { LastProcessedId = lastProcessedId });
        }
    }
}
