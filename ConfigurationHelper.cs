using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System;
using System.Configuration;
using System.Collections.Specialized;
using Microsoft.Extensions.Logging;

namespace HealthcareTransparencyParser
{
    /// <summary>
    /// Helper class to access configuration settings from app.config
    /// </summary>
    public class ConfigurationHelper
    {
        private readonly ILogger<ConfigurationHelper> _logger;
        private readonly string _environment;
        private readonly NameValueCollection _environmentSettings;

        public ConfigurationHelper(ILogger<ConfigurationHelper> logger)
        {
            _logger = logger;

            // Get the current environment from app.config
            _environment = ConfigurationManager.AppSettings["Environment"] ?? "Development";
            _logger.LogInformation($"Using environment: {_environment}");

            // Get environment-specific settings
            _environmentSettings = (NameValueCollection)ConfigurationManager.GetSection("environmentSettings");
        }

        /// <summary>
        /// Gets a connection string for the current environment
        /// </summary>
        public string ConnectionString => GetEnvironmentSetting("ConnectionString");

        /// <summary>
        /// Gets whether mock data generation is enabled for the current environment
        /// </summary>
        public bool MockDataEnabled => bool.Parse(GetEnvironmentSetting("MockDataEnabled") ?? "false");

        /// <summary>
        /// Gets the number of mock providers to generate for the current environment
        /// </summary>
        public int MockProvidersCount => int.Parse(GetEnvironmentSetting("MockProvidersCount") ?? "0");

        /// <summary>
        /// Gets the batch size for processing data in the current environment
        /// </summary>
        public int ProcessingBatchSize => int.Parse(GetEnvironmentSetting("ProcessingBatchSize") ??
                                                   ConfigurationManager.AppSettings["DefaultBatchSize"] ?? "1000");

        /// <summary>
        /// Gets the data file URL for the current environment
        /// </summary>
        public string DataFileUrl => GetEnvironmentSetting("DataFileUrl");

        /// <summary>
        /// Gets the default schema for the current environment
        /// </summary>
        public string DefaultSchema => ConfigurationManager.AppSettings["DefaultSchema"] ?? "providers-reference";

        /// <summary>
        /// Gets the HTTP timeout in minutes
        /// </summary>
        public int HttpTimeoutMinutes => int.Parse(ConfigurationManager.AppSettings["HttpTimeoutMinutes"] ?? "120");

        /// <summary>
        /// Gets whether automatic retry is enabled
        /// </summary>
        public bool EnableAutomaticRetry => bool.Parse(ConfigurationManager.AppSettings["EnableAutomaticRetry"] ?? "true");

        /// <summary>
        /// Gets the maximum number of retry attempts
        /// </summary>
        public int MaxRetryAttempts => int.Parse(ConfigurationManager.AppSettings["MaxRetryAttempts"] ?? "3");


        /// <summary>
        /// Gets whether to load the Providers table
        /// </summary>
        public bool LoadProvidersTable => bool.Parse(GetEnvironmentSetting("LoadProvidersTable") ?? "true");

        /// <summary>
        /// Gets whether to load the AllowedAmounts table
        /// </summary>
        public bool LoadAllowedAmountsTable => bool.Parse(GetEnvironmentSetting("LoadAllowedAmountsTable") ?? "true");

        /// <summary>
        /// Gets whether to load the InNetworkRates table
        /// </summary>
        public bool LoadInNetworkRatesTable => bool.Parse(GetEnvironmentSetting("LoadInNetworkRatesTable") ?? "true");

        /// <summary>
        /// Gets the seed value for mock data generation
        /// </summary>
        public int MockDataSeed => int.Parse(GetEnvironmentSetting("MockDataSeed") ?? "0");

        /// <summary>
        /// Gets the log level
        /// </summary>
        public LogLevel LogLevel
        {
            get
            {
                var logLevelString = ConfigurationManager.AppSettings["LogLevel"] ?? "Information";
                return Enum.TryParse<LogLevel>(logLevelString, out var level) ? level : LogLevel.Information;
            }
        }

        /// <summary>
        /// Gets whether detailed errors are enabled
        /// </summary>
        public bool EnableDetailedErrors => bool.Parse(ConfigurationManager.AppSettings["EnableDetailedErrors"] ?? "false");

        /// <summary>
        /// Gets an environment-specific setting
        /// </summary>
        /// <param name="key">The setting key without environment prefix</param>
        /// <returns>The setting value or null if not found</returns>
        private string GetEnvironmentSetting(string key)
        {
            var fullKey = $"{_environment}.{key}";
            return _environmentSettings[fullKey];
        }
    }
}
