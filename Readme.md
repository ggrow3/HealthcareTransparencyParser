# Healthcare Transparency Data Parser

A .NET 9 application designed to efficiently process large healthcare transparency data files into Azure SQL Database.

## Overview

The Healthcare Transparency Data Parser is a high-performance console application that can:
- Download and process extremely large gzipped JSON files (4GB zipped, up to 100GB unzipped)
- Parse healthcare transparency data according to CMS schemas
- Store data in Azure SQL Database with proper schema mapping
- Process data in memory-efficient batches to work with limited RAM (16GB)
- Implement idempotent processing that can resume from failure
- Ensure data integrity with transaction-based operations
- Avoid duplicating provider records

## Supported Schemas

The application currently supports the following schemas:
- `providers-reference` - Healthcare provider reference data

## Requirements

- .NET 9 SDK
- Azure SQL Database
- 16GB RAM minimum
- Sufficient disk space for temporary file processing

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/ggrow3/healthcare-transparency-parser.git
   cd healthcare-transparency-parser
   ```

2. Build the application:
   ```
   dotnet build -c Release
   ```

## Usage

```
dotnet run -- --url <url-to-gzipped-json> --schema providers-reference --connection-string <azure-sql-connection-string> --batch-size 1000
```

### Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `--url` | URL to the gzipped JSON file | Yes | - |
| `--schema` | Schema type to process (providers-reference) | Yes | - |
| `--connection-string` | Azure SQL Database connection string | Yes | - |
| `--batch-size` | Number of records to process in each batch | No | 1000 |

## Architecture

The application follows a modular design with clear separation of concerns:

- **Program**: Command-line interface and dependency injection setup
- **JsonProcessor**: Core processing orchestration
- **SchemaHandlers**: Schema-specific data parsing and database operations
- **ProcessingStateManager**: Handles processing state tracking for idempotency

## Database Schema

The application creates the following tables in the `transparency` schema:

### transparency.Providers
Stores healthcare provider information including identifiers, names, and organization details.

### transparency.ProviderAddresses
Stores provider address information with references to the Providers table.

### transparency.ProcessingState
Tracks processing state for idempotent operations, enabling restart after failure.

## Performance Considerations

- The application uses streaming JSON parsing to minimize memory usage
- Batch size can be adjusted based on available memory (smaller batches use less memory)
- Database operations use efficient Dapper ORM with batch processing
- Progress reporting is provided for long-running operations

## Extending the Application

To add support for additional schemas:
1. Define the schema-specific data models
2. Implement a new schema handler that implements the `ISchemaHandler` interface
3. Update the `SchemaHandlerFactory` to handle the new schema type
