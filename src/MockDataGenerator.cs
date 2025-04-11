using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace HealthcareTransparencyParser
{
    /// <summary>
    /// Generates mock healthcare transparency data for testing
    /// </summary>
    public class MockDataGenerator
    {
        private readonly Random _random = new Random();
        private readonly ILogger<MockDataGenerator> _logger;

        // Constants for realistic data generation
        private readonly string[] _firstNames = { "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth" };
        private readonly string[] _lastNames = { "Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor" };
        private readonly string[] _addressTypes = { "LOCATION", "BILLING", "MAILING" };
        private readonly string[] _states = { "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA" };
        private readonly string[] _cities = { "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego" };
        private readonly string[] _entityTypes = { "INDIVIDUAL", "GROUP", "HOSPITAL", "CLINIC" };
        private readonly string[] _tinTypes = { "EIN", "SSN", "ITIN" };
        private readonly string[] _billingCodes = { "99203", "99212", "99213", "99214", "90791", "90837", "97110", "97140", "29125", "29515" };
        private readonly string[] _billingCodeTypes = { "CPT", "HCPCS", "ICD10", "DRG", "MS-DRG", "R-DRG", "APC" };
        private readonly string[] _negotiationTypes = { "negotiated", "fee_schedule", "percentage", "per_diem" };
        private readonly string[] _billingClasses = { "professional", "institutional", "pharmacy" };
        private readonly string[] _currencyCodes = { "USD" };
        private readonly string[] _currencyUnits = { "DOLLARS" };
        private readonly string[] _serviceCodes = { "OFFICE VISIT", "EMERGENCY", "RADIOLOGY", "LABORATORY", "SURGERY", "PREVENTIVE" };

        public MockDataGenerator(ILogger<MockDataGenerator> logger, ConfigurationHelper config)
        {
            _logger = logger;
            // Use seed from config if provided, otherwise use time-based seed
            _random = config.MockDataSeed > 0 ? new Random(config.MockDataSeed) : new Random();
        }


        /// <summary>
        /// Generates a mock providers-reference gzipped JSON file with random data
        /// </summary>
        public async Task GenerateProvidersReferenceFileAsync(string filePath, int providerCount)
        {
            var providers = GenerateRandomProviders(providerCount);
            var data = new { providers = providers };

            // Create a JSON string with proper formatting
            var options = new JsonSerializerOptions { WriteIndented = true };
            var jsonString = JsonSerializer.Serialize(data, options);

            // Compress to gzip file
            using var fileStream = new FileStream(filePath, FileMode.Create);
            using var gzipStream = new GZipStream(fileStream, CompressionMode.Compress);
            using var writer = new StreamWriter(gzipStream);

            await writer.WriteAsync(jsonString);
        }

        /// <summary>
        /// Generates a mock allowed-amounts gzipped JSON file with random data
        /// </summary>
        public async Task GenerateAllowedAmountsFileAsync(string filePath, int itemCount)
        {
            var allowedAmounts = GenerateRandomAllowedAmounts(itemCount);
            var data = new { allowed_amounts = allowedAmounts };

            // Create a JSON string with proper formatting
            var options = new JsonSerializerOptions { WriteIndented = true };
            var jsonString = JsonSerializer.Serialize(data, options);

            // Compress to gzip file
            using var fileStream = new FileStream(filePath, FileMode.Create);
            using var gzipStream = new GZipStream(fileStream, CompressionMode.Compress);
            using var writer = new StreamWriter(gzipStream);

            await writer.WriteAsync(jsonString);
        }

        /// <summary>
        /// Generates a mock in-network-rates gzipped JSON file with random data
        /// </summary>
        public async Task GenerateInNetworkRatesFileAsync(string filePath, int itemCount)
        {
            var inNetworkRates = GenerateRandomInNetworkRates(itemCount);
            var data = new { in_network = inNetworkRates };

            // Create a JSON string with proper formatting
            var options = new JsonSerializerOptions { WriteIndented = true };
            var jsonString = JsonSerializer.Serialize(data, options);

            // Compress to gzip file
            using var fileStream = new FileStream(filePath, FileMode.Create);
            using var gzipStream = new GZipStream(fileStream, CompressionMode.Compress);
            using var writer = new StreamWriter(gzipStream);

            await writer.WriteAsync(jsonString);
        }

        /// <summary>
        /// Generates a list of random provider objects
        /// </summary>
        private List<Dictionary<string, object>> GenerateRandomProviders(int count)
        {
            var providers = new List<Dictionary<string, object>>();

            for (int i = 0; i < count; i++)
            {
                var providerId = $"PROVIDER_{i:D6}";
                var npi = GenerateRandomNumber(10);
                var entityType = GetRandomElement(_entityTypes);

                // Determine if individual or organization
                bool isIndividual = entityType == "INDIVIDUAL";

                var provider = new Dictionary<string, object>
                {
                    ["provider_id"] = providerId,
                    ["npi"] = npi,
                    ["entity_type"] = entityType,
                    ["tin"] = new Dictionary<string, object>
                    {
                        ["type"] = GetRandomElement(_tinTypes),
                        ["value"] = GenerateRandomNumber(9)
                    }
                };

                // Add name details based on entity type
                if (isIndividual)
                {
                    provider["first_name"] = GetRandomElement(_firstNames);

                    // Randomly include middle name
                    if (_random.Next(2) == 0)
                    {
                        provider["middle_name"] = GetRandomElement(_firstNames)[0].ToString();
                    }

                    provider["last_name"] = GetRandomElement(_lastNames);

                    // Randomly include suffix
                    if (_random.Next(5) == 0)
                    {
                        provider["suffix"] = GetRandomSuffix();
                    }
                }
                else
                {
                    provider["name"] = GenerateOrganizationName();
                }

                // Add addresses (1-3 random addresses)
                var addressCount = _random.Next(1, 4);
                var addresses = new List<Dictionary<string, object>>();

                for (int j = 0; j < addressCount; j++)
                {
                    addresses.Add(GenerateRandomAddress());
                }

                provider["addresses"] = addresses;

                providers.Add(provider);
            }

            return providers;
        }

        /// <summary>
        /// Generates a list of random allowed-amounts objects
        /// </summary>
        private List<Dictionary<string, object>> GenerateRandomAllowedAmounts(int count)
        {
            var allowedAmounts = new List<Dictionary<string, object>>();

            for (int i = 0; i < count; i++)
            {
                var item = new Dictionary<string, object>
                {
                    ["reporting_entity_name"] = GenerateOrganizationName(),
                    ["reporting_entity_type"] = GetRandomElement(_entityTypes),
                    ["last_updated_on"] = DateTime.Now.AddDays(-_random.Next(1, 365)).ToString("yyyy-MM-dd"),
                    ["version"] = $"1.{_random.Next(0, 10)}",
                    ["billing_code"] = GetRandomElement(_billingCodes),
                    ["billing_code_type"] = GetRandomElement(_billingCodeTypes),
                    ["billing_code_type_version"] = $"{_random.Next(1, 10)}.0",
                    ["negotiation_arrangement"] = "ffs",
                    ["description"] = $"Service description for {GetRandomElement(_billingCodes)}"
                };

                // Add providers (1-5 random providers)
                var providerCount = _random.Next(1, 6);
                var providers = new List<Dictionary<string, object>>();

                for (int j = 0; j < providerCount; j++)
                {
                    var provider = new Dictionary<string, object>
                    {
                        ["provider_references"] = new[] { $"PROVIDER_{_random.Next(0, 10000):D6}" },
                        ["npi"] = GenerateRandomNumber(10),
                        ["tin"] = new Dictionary<string, object>
                        {
                            ["type"] = GetRandomElement(_tinTypes),
                            ["value"] = GenerateRandomNumber(9)
                        },
                        ["service_code"] = GetRandomElement(_serviceCodes),
                        ["billing_class"] = GetRandomElement(_billingClasses)
                    };

                    providers.Add(provider);
                }

                item["providers"] = providers;

                // Add allowed amounts (1-3 random amounts)
                var amountsCount = _random.Next(1, 4);
                var amounts = new List<Dictionary<string, object>>();

                for (int j = 0; j < amountsCount; j++)
                {
                    var amount = new Dictionary<string, object>
                    {
                        ["allowed_amount"] = _random.Next(10000, 1000000) / 100.0m,
                        ["billed_service"] = GetRandomElement(_serviceCodes),
                        ["billing_currency"] = new Dictionary<string, object>
                        {
                            ["code"] = GetRandomElement(_currencyCodes),
                            ["unit"] = GetRandomElement(_currencyUnits)
                        },
                        ["expiration_date"] = DateTime.Now.AddYears(1).ToString("yyyy-MM-dd"),
                        ["service_code"] = GetRandomElement(_serviceCodes)
                    };

                    amounts.Add(amount);
                }

                item["allowed_amounts"] = amounts;

                allowedAmounts.Add(item);
            }

            return allowedAmounts;
        }

        /// <summary>
        /// Generates a list of random in-network-rates objects
        /// </summary>
        private List<Dictionary<string, object>> GenerateRandomInNetworkRates(int count)
        {
            var inNetworkRates = new List<Dictionary<string, object>>();

            for (int i = 0; i < count; i++)
            {
                var item = new Dictionary<string, object>
                {
                    ["reporting_entity_name"] = GenerateOrganizationName(),
                    ["reporting_entity_type"] = GetRandomElement(_entityTypes),
                    ["last_updated_on"] = DateTime.Now.AddDays(-_random.Next(1, 365)).ToString("yyyy-MM-dd"),
                    ["version"] = $"1.{_random.Next(0, 10)}",
                    ["billing_code"] = GetRandomElement(_billingCodes),
                    ["billing_code_type"] = GetRandomElement(_billingCodeTypes),
                    ["billing_code_type_version"] = $"{_random.Next(1, 10)}.0",
                    ["negotiation_arrangement"] = "ffs",
                    ["description"] = $"Service description for {GetRandomElement(_billingCodes)}"
                };

                // Randomly add bundled codes
                if (_random.Next(2) == 0)
                {
                    var bundledCodesCount = _random.Next(1, 4);
                    var bundledCodes = new List<Dictionary<string, object>>();

                    for (int j = 0; j < bundledCodesCount; j++)
                    {
                        bundledCodes.Add(new Dictionary<string, object>
                        {
                            ["billing_code"] = GetRandomElement(_billingCodes),
                            ["billing_code_type"] = GetRandomElement(_billingCodeTypes),
                            ["billing_code_type_version"] = $"{_random.Next(1, 10)}.0",
                            ["description"] = $"Bundled service {j + 1}"
                        });
                    }

                    item["bundled_codes"] = bundledCodes;
                }

                // Add negotiated prices (1-3 random prices)
                var pricesCount = _random.Next(1, 4);
                var prices = new List<Dictionary<string, object>>();

                for (int j = 0; j < pricesCount; j++)
                {
                    var price = new Dictionary<string, object>
                    {
                        ["provider_group_id"] = $"GROUP_{_random.Next(1, 1000):D3}",
                        ["provider_references"] = GenerateRandomProviderReferences(_random.Next(1, 5))
                    };

                    // Add negotiated rates (1-3 random rates)
                    var ratesCount = _random.Next(1, 4);
                    var rates = new List<Dictionary<string, object>>();

                    for (int k = 0; k < ratesCount; k++)
                    {
                        var rate = new Dictionary<string, object>
                        {
                            ["negotiated_type"] = GetRandomElement(_negotiationTypes),
                            ["negotiated_rate"] = _random.Next(10000, 1000000) / 100.0m,
                            ["expiration_date"] = DateTime.Now.AddYears(1).ToString("yyyy-MM-dd"),
                            ["service_code"] = GetRandomElement(_serviceCodes),
                            ["billing_currency"] = new Dictionary<string, object>
                            {
                                ["code"] = GetRandomElement(_currencyCodes),
                                ["unit"] = GetRandomElement(_currencyUnits)
                            }
                        };

                        // Randomly add additional information
                        if (_random.Next(2) == 0)
                        {
                            rate["additional_information"] = $"Additional info for rate {k + 1}";
                        }

                        rates.Add(rate);
                    }

                    price["negotiated_rates"] = rates;
                    prices.Add(price);
                }

                item["negotiated_prices"] = prices;

                inNetworkRates.Add(item);
            }

            return inNetworkRates;
        }

        /// <summary>
        /// Generates random provider references
        /// </summary>
        private string[] GenerateRandomProviderReferences(int count)
        {
            var references = new string[count];
            for (int i = 0; i < count; i++)
            {
                references[i] = $"PROVIDER_{_random.Next(0, 10000):D6}";
            }
            return references;
        }

        /// <summary>
        /// Generates a random address
        /// </summary>
        private Dictionary<string, object> GenerateRandomAddress()
        {
            var address = new Dictionary<string, object>
            {
                ["address_type"] = GetRandomElement(_addressTypes),
                ["address_1"] = $"{_random.Next(100, 9999)} {GetRandomElement(_lastNames)} {GetRandomStreetType()}",
                ["city"] = GetRandomElement(_cities),
                ["state"] = GetRandomElement(_states),
                ["zip_code"] = $"{_random.Next(10000, 99999)}"
            };

            // Randomly include address_2
            if (_random.Next(3) == 0)
            {
                address["address_2"] = GetRandomAddressLine2();
            }

            return address;
        }

        /// <summary>
        /// Gets a random element from an array
        /// </summary>
        private T GetRandomElement<T>(T[] array)
        {
            return array[_random.Next(array.Length)];
        }

        /// <summary>
        /// Generates a random numerical string of specified length
        /// </summary>
        /// <summary>
        /// Generates a random numerical string of specified length
        /// </summary>
        private string GenerateRandomNumber(int length)
        {
            var sb = new StringBuilder(length);
            for (int i = 0; i < length; i++)
            {
                sb.Append(_random.Next(10));
            }
            return sb.ToString();
        }

        /// <summary>
        /// Generates a random organization name
        /// </summary>
        private string GenerateOrganizationName()
        {
            string[] prefixes = { "United", "National", "American", "Regional", "Community", "Advanced", "Premier", "Elite", "Integrated" };
            string[] middles = { "Health", "Medical", "Care", "Wellness", "Healthcare", "Physicians", "Specialty", "Family" };
            string[] suffixes = { "Group", "Center", "Associates", "Partners", "Network", "Services", "Clinic", "System", "Providers" };

            return $"{GetRandomElement(prefixes)} {GetRandomElement(middles)} {GetRandomElement(suffixes)}";
        }

        /// <summary>
        /// Generates a random street type
        /// </summary>
        private string GetRandomStreetType()
        {
            string[] streetTypes = { "St", "Ave", "Blvd", "Dr", "Ln", "Rd", "Way", "Pl", "Ct" };
            return GetRandomElement(streetTypes);
        }

        /// <summary>
        /// Generates a random address line 2
        /// </summary>
        private string GetRandomAddressLine2()
        {
            string[] prefixes = { "Suite", "Apt", "Unit", "Building", "Floor" };
            return $"{GetRandomElement(prefixes)} {_random.Next(1, 1000)}";
        }

        /// <summary>
        /// Generates a random name suffix
        /// </summary>
        private string GetRandomSuffix()
        {
            string[] suffixes = { "Jr.", "Sr.", "III", "IV", "M.D.", "Ph.D." };
            return GetRandomElement(suffixes);
        }
    }
}