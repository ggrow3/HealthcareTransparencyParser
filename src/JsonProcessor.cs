using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;



    namespace HealthcareTransparencyParser
    {
        /// <summary>
        /// Main processor class that orchestrates the JSON processing workflow
        /// </summary>
        public class JsonProcessor
        {
            private readonly ILogger<JsonProcessor> _logger;
            private readonly IHttpClientFactory _httpClientFactory;
            private readonly ISchemaHandlerFactory _schemaHandlerFactory;
            private readonly IProcessingStateManager _stateManager;

            public JsonProcessor(
                ILogger<JsonProcessor> logger,
                IHttpClientFactory httpClientFactory,
                ISchemaHandlerFactory schemaHandlerFactory,
                IProcessingStateManager stateManager)
            {
                _logger = logger;
                _httpClientFactory = httpClientFactory;
                _schemaHandlerFactory = schemaHandlerFactory;
                _stateManager = stateManager;
            }

            public async Task ProcessFileAsync(string schemaType, string filePathOrUrl, string connectionString, int batchSize)
            {
                _logger.LogInformation($"Processing {schemaType} file from {filePathOrUrl}");

                // Create schema handler
                var schemaHandler = _schemaHandlerFactory.CreateHandler(schemaType);
                if (schemaHandler == null)
                {
                    throw new ArgumentException($"Unsupported schema type: {schemaType}");
                }

                // Prepare database
                _logger.LogInformation("Setting up database tables...");
                await schemaHandler.SetupDatabaseAsync(connectionString);

                // Check existing processing state
                var state = await _stateManager.GetProcessingStateAsync(connectionString, filePathOrUrl, schemaType);
                if (state.IsCompleted)
                {
                    _logger.LogInformation($"File {filePathOrUrl} was already processed successfully. Skipping.");
                    return;
                }

                // Process the file
                var isLocalFile = File.Exists(filePathOrUrl);
                string tempFilePath = isLocalFile ? filePathOrUrl : Path.GetTempFileName();

                try
                {
                    // Download the file if it's a URL
                    if (!isLocalFile)
                    {
                        _logger.LogInformation($"Downloading file to {tempFilePath}...");
                        await DownloadFileAsync(filePathOrUrl, tempFilePath);
                    }
                    else
                    {
                        _logger.LogInformation($"Using local file: {filePathOrUrl}");
                    }

                    // Process the file
                    _logger.LogInformation("Processing file...");
                    await ProcessGzippedJsonFileAsync(tempFilePath, connectionString, schemaHandler, state, batchSize);

                    // Mark processing as completed
                    await _stateManager.MarkProcessingCompletedAsync(connectionString, filePathOrUrl, schemaType);

                    _logger.LogInformation($"Successfully processed {schemaType} file from {filePathOrUrl}");
                }
                finally
                {
                    // Clean up temp file only if we created it
                    if (!isLocalFile && File.Exists(tempFilePath))
                    {
                        File.Delete(tempFilePath);
                        _logger.LogInformation($"Deleted temporary file: {tempFilePath}");
                    }
                }
            }

            private async Task DownloadFileAsync(string url, string destinationPath)
            {
                using var httpClient = _httpClientFactory.CreateClient("downloader");

                // Set a timeout for large files
                httpClient.Timeout = TimeSpan.FromHours(2);

                // Download with progress reporting
                using var response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
                response.EnsureSuccessStatusCode();

                var totalBytes = response.Content.Headers.ContentLength ?? -1L;

                using var contentStream = await response.Content.ReadAsStreamAsync();
                using var fileStream = new FileStream(destinationPath, FileMode.Create, FileAccess.Write, FileShare.None,
                    bufferSize: 8192, useAsync: true);

                var buffer = new byte[8192];
                var bytesRead = 0;
                var totalBytesRead = 0L;
                var lastReportTime = DateTime.Now;

                while ((bytesRead = await contentStream.ReadAsync(buffer)) > 0)
                {
                    await fileStream.WriteAsync(buffer.AsMemory(0, bytesRead));
                    totalBytesRead += bytesRead;

                    // Report progress every 5 seconds
                    if ((DateTime.Now - lastReportTime).TotalSeconds >= 5)
                    {
                        if (totalBytes > 0)
                        {
                            var percentComplete = (double)totalBytesRead / totalBytes * 100;
                            _logger.LogInformation($"Download progress: {percentComplete:F2}% ({FormatBytes(totalBytesRead)} of {FormatBytes(totalBytes)})");
                        }
                        else
                        {
                            _logger.LogInformation($"Downloaded {FormatBytes(totalBytesRead)}");
                        }

                        lastReportTime = DateTime.Now;
                    }
                }
            }

            private string FormatBytes(long bytes)
            {
                string[] suffixes = { "B", "KB", "MB", "GB", "TB" };
                int counter = 0;
                decimal number = bytes;
                while (Math.Round(number / 1024) >= 1)
                {
                    number /= 1024;
                    counter++;
                }
                return $"{number:F2} {suffixes[counter]}";
            }

            private async Task ProcessGzippedJsonFileAsync(string filePath, string connectionString,
                ISchemaHandler schemaHandler, ProcessingState state, int batchSize)
            {
                using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
                using var gzipStream = new GZipStream(fileStream, CompressionMode.Decompress);

                // Use a custom stream processor to handle extremely large files
                await schemaHandler.ProcessStreamAsync(gzipStream, connectionString, state.LastProcessedId, batchSize);
            }
        }
    }

