using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace HealthcareTransparencyParser
{
    /// <summary>
    /// Interface for managing processing state
    /// </summary>
    public interface IProcessingStateManager
    {
        Task<ProcessingState> GetProcessingStateAsync(string connectionString, string url, string schemaType);
        Task MarkProcessingCompletedAsync(string connectionString, string url, string schemaType);
    }

    /// <summary>
    /// Manages processing state for idempotency
    /// </summary>
    public class ProcessingStateManager : IProcessingStateManager
    {
        private readonly ILogger<ProcessingStateManager> _logger;

        public ProcessingStateManager(ILogger<ProcessingStateManager> logger)
        {
            _logger = logger;
        }

        public async Task<ProcessingState> GetProcessingStateAsync(string connectionString, string url, string schemaType)
        {
            using var connection = new SqlConnection(connectionString);

            // Check if we have a processing state for this URL and schema
            var state = await connection.QueryFirstOrDefaultAsync<ProcessingState>(@"
SELECT Id, Url, SchemaType, LastProcessedId, IsCompleted, StartedAt, CompletedAt
FROM transparency.ProcessingState
WHERE Url = @Url AND SchemaType = @SchemaType", new { Url = url, SchemaType = schemaType });

            if (state == null)
            {
                // Create new processing state
                state = new ProcessingState
                {
                    Id = Guid.NewGuid(),
                    Url = url,
                    SchemaType = schemaType,
                    LastProcessedId = null,
                    IsCompleted = false,
                    StartedAt = DateTime.UtcNow,
                    CompletedAt = null
                };

                await connection.ExecuteAsync(@"
INSERT INTO transparency.ProcessingState (Id, Url, SchemaType, LastProcessedId, IsCompleted, StartedAt)
VALUES (@Id, @Url, @SchemaType, @LastProcessedId, @IsCompleted, @StartedAt)",
                    state);

                _logger.LogInformation($"Created new processing state for {url} with schema {schemaType}");
            }
            else
            {
                _logger.LogInformation($"Found existing processing state for {url}, last processed ID: {state.LastProcessedId ?? "none"}");
            }

            return state;
        }

        public async Task MarkProcessingCompletedAsync(string connectionString, string url, string schemaType)
        {
            using var connection = new SqlConnection(connectionString);

            await connection.ExecuteAsync(@"
UPDATE transparency.ProcessingState
SET IsCompleted = 1, CompletedAt = @CompletedAt
WHERE Url = @Url AND SchemaType = @SchemaType",
                new { Url = url, SchemaType = schemaType, CompletedAt = DateTime.UtcNow });

            _logger.LogInformation($"Marked processing as completed for {url} with schema {schemaType}");
        }
    }
}