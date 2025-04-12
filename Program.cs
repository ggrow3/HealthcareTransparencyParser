using System;
using System.CommandLine;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Configuration;
using Polly;
using Polly.Retry;
using System.Linq;

namespace HealthcareTransparencyParser
{
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            // Set up command line options
            var rootCommand = new RootCommand("Healthcare Transparency Data Parser");

            var urlOption = new Option<string>(
                "--url",
                "URL to the gzipped JSON file to process"
            );

            var schemaOption = new Option<string[]>(
                "--schema",
                "JSON schema type(s) to process (providers-reference, allowed-amounts, in-network-rates)"
            )
            { AllowMultipleArgumentsPerToken = true };

            var connectionStringOption = new Option<string>(
                "--connection-string",
                "Azure SQL Database connection string"
            );

            var batchSizeOption = new Option<int>(
                "--batch-size",
                "Number of records to process in each batch"
            );

            var useMockDataOption = new Option<bool>(
                "--use-mock-data",
                "Generate and use mock data instead of downloading from URL"
            );

            var mockProvidersCountOption = new Option<int>(
                "--mock-providers-count",
                "Number of mock providers to generate (when using --use-mock-data)"
            );

            var environmentOption = new Option<string>(
                "--environment",
                "Environment to use (Development, Testing, Production)"
            );

            rootCommand.AddOption(urlOption);
            rootCommand.AddOption(schemaOption);
            rootCommand.AddOption(connectionStringOption);
            rootCommand.AddOption(batchSizeOption);
            rootCommand.AddOption(useMockDataOption);
            rootCommand.AddOption(mockProvidersCountOption);
            rootCommand.AddOption(environmentOption);

            rootCommand.SetHandler(async (url, schemas, connectionString, batchSize, useMockData, mockProvidersCount, environment) =>
            {
                IHost host = null;

                try
                {
                    // Set environment if specified
                    if (!string.IsNullOrEmpty(environment))
                    {
                        ConfigurationManager.AppSettings["Environment"] = environment;
                    }

                    // Create host and run the application
                    host = CreateHostBuilder(args).Build();

                    // Get configuration
                    var config = host.Services.GetRequiredService<ConfigurationHelper>();

                    // Apply configuration values if command-line arguments are not provided
                    if (string.IsNullOrEmpty(url))
                    {
                        url = config.DataFileUrl;
                    }

                    // Get default schema if none specified
                    if (schemas == null || schemas.Length == 0)
                    {
                        schemas = config.DefaultSchema.Split(';');
                    }

                    if (string.IsNullOrEmpty(connectionString))
                    {
                        connectionString = config.ConnectionString;

                        if (string.IsNullOrEmpty(connectionString))
                        {
                            Console.Error.WriteLine("Error: Connection string not provided and not found in configuration");
                            Environment.ExitCode = 1;
                            return;
                        }
                    }

                    // If batch size not specified, use the one from config
                    if (batchSize <= 0)
                    {
                        batchSize = config.ProcessingBatchSize;
                    }

                    // Use mock data from config if not specified in command line
                    if (!useMockData)
                    {
                        useMockData = config.MockDataEnabled;
                    }

                    // Use mock providers count from config if not specified in command line
                    if (mockProvidersCount <= 0 && useMockData)
                    {
                        mockProvidersCount = config.MockProvidersCount;
                    }

                    var supportedSchemas = new List<string>() { "providers-reference", "allowed-amounts", "in-network-rates" };
                    var mockDataGenerator = host.Services.GetRequiredService<MockDataGenerator>();
                    var processor = host.Services.GetRequiredService<JsonProcessor>();
                    var tempFiles = new List<string>();

                    // Process each schema
                    foreach (var schema in schemas)
                    {
                        string schemaLower = schema.ToLower();

                        if (!supportedSchemas.Contains(schemaLower))
                        {
                            Console.WriteLine($"Warning: Schema '{schema}' is not supported. Skipping.");
                            continue;
                        }

                        string schemaUrl = url;

                        // Check if table should be loaded based on schema type
                        bool shouldLoad = schemaLower switch
                        {
                            "providers-reference" => config.LoadProvidersTable,
                            "allowed-amounts" => config.LoadAllowedAmountsTable,
                            "in-network-rates" => config.LoadInNetworkRatesTable,
                            _ => true
                        };

                        if (!shouldLoad)
                        {
                            Console.WriteLine($"Skipping {schema} processing (disabled in configuration)");
                            continue;
                        }

                        // Handle mock data generation for this schema
                        if (useMockData)
                        {
                            // Generate a unique temporary file for each schema
                            schemaUrl = Path.Combine(Path.GetTempPath(), $"mock_{schemaLower}_data_{Guid.NewGuid()}.json.gz");
                            tempFiles.Add(schemaUrl);
                            Console.WriteLine($"Generating mock data file for {schemaLower}: {schemaUrl}");

                            // Generate appropriate mock data based on schema
                            if (schemaLower == "providers-reference" && config.LoadProvidersTable)
                            {
                                await mockDataGenerator.GenerateProvidersReferenceFileAsync(schemaUrl, mockProvidersCount);
                                Console.WriteLine($"Generated mock providers-reference data with {mockProvidersCount} providers");
                            }
                            else if (schemaLower == "allowed-amounts" && config.LoadAllowedAmountsTable)
                            {
                                await mockDataGenerator.GenerateAllowedAmountsFileAsync(schemaUrl, mockProvidersCount);
                                Console.WriteLine($"Generated mock allowed-amounts data with {mockProvidersCount} items");
                            }
                            else if (schemaLower == "in-network-rates" && config.LoadInNetworkRatesTable)
                            {
                                await mockDataGenerator.GenerateInNetworkRatesFileAsync(schemaUrl, mockProvidersCount);
                                Console.WriteLine($"Generated mock in-network-rates data with {mockProvidersCount} items");
                            }
                        }
                        else if (string.IsNullOrEmpty(schemaUrl))
                        {
                            Console.WriteLine($"Skipping {schema}: URL is required unless mock data is enabled");
                            continue;
                        }

                        // Process this schema
                        Console.WriteLine($"Processing schema: {schema}");
                        await processor.ProcessFileAsync(schema, schemaUrl, connectionString, batchSize);
                        Console.WriteLine($"Completed processing {schema}");
                    }

                    // Clean up all temporary files
                    foreach (var tempFile in tempFiles)
                    {
                        if (File.Exists(tempFile))
                        {
                            try
                            {
                                File.Delete(tempFile);
                                Console.WriteLine($"Deleted temporary mock data file: {tempFile}");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Warning: Failed to delete temporary file: {ex.Message}");
                            }
                        }
                    }

                    Console.WriteLine("All processing completed successfully.");
                    Environment.ExitCode = 0;
                }
                catch (Exception ex)
                {
                    ConfigurationHelper config = null;

                    // Try to get the configuration if host exists
                    if (host != null)
                    {
                        try
                        {
                            config = host.Services.GetService<ConfigurationHelper>();
                        }
                        catch
                        {
                            // Ignore errors trying to get config in error handler
                        }
                    }

                    bool showDetailedErrors = config?.EnableDetailedErrors ?? true;

                    Console.Error.WriteLine($"Error: {ex.Message}");

                    if (showDetailedErrors)
                    {
                        Console.Error.WriteLine(ex.StackTrace);

                        if (ex.InnerException != null)
                        {
                            Console.Error.WriteLine($"Inner exception: {ex.InnerException.Message}");
                            Console.Error.WriteLine(ex.InnerException.StackTrace);
                        }
                    }

                    Environment.ExitCode = 1;
                }
            }, urlOption, schemaOption, connectionStringOption, batchSizeOption, useMockDataOption, mockProvidersCountOption, environmentOption);

            return await rootCommand.InvokeAsync(args);
        }

        static IHostBuilder CreateHostBuilder(string[] args) =>
     Host.CreateDefaultBuilder(args)
         .ConfigureServices((hostContext, services) =>
         {
             // Add configuration helper
             services.AddSingleton<ConfigurationHelper>();

             // Configure HTTP client with settings from configuration
             services.AddHttpClient("downloader", (provider, client) =>
             {
                 var config = provider.GetRequiredService<ConfigurationHelper>();
                 client.Timeout = TimeSpan.FromMinutes(config.HttpTimeoutMinutes);
             })
             .AddTransientHttpErrorPolicy(policy =>
             {
                 var serviceProvider = services.BuildServiceProvider();
                 var config = serviceProvider.GetRequiredService<ConfigurationHelper>();

                 if (config.EnableAutomaticRetry)
                 {
                     return policy.WaitAndRetryAsync(
                         config.MaxRetryAttempts,
                         retry => TimeSpan.FromSeconds(Math.Pow(2, retry)));
                 }
                 else
                 {
                     return policy.WaitAndRetryAsync(0, _ => TimeSpan.Zero);
                 }
             });

             // Add core services
             services.AddTransient<JsonProcessor>();
             services.AddTransient<ISchemaHandlerFactory, SchemaHandlerFactory>();

             // Add schema handlers
             services.AddTransient<IProvidersReferenceHandler, ProvidersReferenceHandler>();
             services.AddTransient<IAllowedAmountsHandler, AllowedAmountsHandler>();
             services.AddTransient<IInNetworkRatesHandler, InNetworkRatesHandler>();

             services.AddTransient<IProcessingStateManager, ProcessingStateManager>();
             services.AddTransient<MockDataGenerator>();

             // Configure logging
             services.AddLogging(logging =>
             {
                 var serviceProvider = services.BuildServiceProvider();
                 var config = serviceProvider.GetRequiredService<ConfigurationHelper>();

                 logging.SetMinimumLevel(config.LogLevel);
             });
         });
    }
}