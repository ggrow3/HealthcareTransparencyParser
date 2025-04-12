-- Drop tables in reverse order of dependencies to avoid foreign key constraint errors

-- First, drop provider references and rates tables that depend on other tables
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'InNetworkRates' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    DROP TABLE transparency.InNetworkRates;
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'InNetworkProviderReferences' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    DROP TABLE transparency.InNetworkProviderReferences;
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'AllowedAmountsRates' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    DROP TABLE transparency.AllowedAmountsRates;
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'AllowedAmountsProviders' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    DROP TABLE transparency.AllowedAmountsProviders;
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'ProviderAddresses' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    DROP TABLE transparency.ProviderAddresses;
END

-- Next, drop the provider groups and items tables
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'InNetworkProviderGroups' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    DROP TABLE transparency.InNetworkProviderGroups;
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'InNetworkRatesItems' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    DROP TABLE transparency.InNetworkRatesItems;
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'AllowedAmountsItems' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    DROP TABLE transparency.AllowedAmountsItems;
END

-- Drop the core providers table
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'Providers' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    DROP TABLE transparency.Providers;
END

-- Finally, drop the processing state table
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'ProcessingState' AND schema_id = SCHEMA_ID('transparency'))
BEGIN
    DROP TABLE transparency.ProcessingState;
END

-- Drop the schema if no tables remain in it
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('transparency'))
BEGIN
    DROP SCHEMA IF EXISTS transparency;
END

PRINT 'All Healthcare Transparency Data Parser tables have been dropped.';