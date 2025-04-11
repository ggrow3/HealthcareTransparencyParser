using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HealthcareTransparencyParser
{
    /// <summary>
    /// Interface for schema-specific handlers
    /// </summary>
    public interface ISchemaHandler
    {
        Task SetupDatabaseAsync(string connectionString);
        Task ProcessStreamAsync(Stream stream, string connectionString, string lastProcessedId, int batchSize);
    }

    /// <summary>
    /// Factory to create specific schema handlers
    /// </summary>
    public interface ISchemaHandlerFactory
    {
        ISchemaHandler CreateHandler(string schemaType);
    }

    /// <summary>
    /// Factory to create specific schema handlers
    /// </summary>
    public class SchemaHandlerFactory : ISchemaHandlerFactory
    {
        private readonly IProvidersReferenceHandler _providersReferenceHandler;
        private readonly IAllowedAmountsHandler _allowedAmountsHandler;
        private readonly IInNetworkRatesHandler _inNetworkRatesHandler;
        private readonly ILogger<SchemaHandlerFactory> _logger;

        public SchemaHandlerFactory(
            IProvidersReferenceHandler providersReferenceHandler,
            IAllowedAmountsHandler allowedAmountsHandler,
            IInNetworkRatesHandler inNetworkRatesHandler,
            ILogger<SchemaHandlerFactory> logger)
        {
            _providersReferenceHandler = providersReferenceHandler;
            _allowedAmountsHandler = allowedAmountsHandler;
            _inNetworkRatesHandler = inNetworkRatesHandler;
            _logger = logger;
        }

        public ISchemaHandler CreateHandler(string schemaType)
        {
            _logger.LogInformation($"Creating handler for schema type: {schemaType}");

            return schemaType.ToLower() switch
            {
                "providers-reference" => _providersReferenceHandler,
                "allowed-amounts" => _allowedAmountsHandler,
                "in-network-rates" => _inNetworkRatesHandler,
                _ => throw new NotSupportedException($"Schema type '{schemaType}' is not supported.")
            };
        }
    }

    /// <summary>
    /// Interface for providers-reference schema handler
    /// </summary>
    public interface IProvidersReferenceHandler : ISchemaHandler
    {
    }
}
