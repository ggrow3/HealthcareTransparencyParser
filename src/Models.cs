using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System;
using System.Collections.Generic;

namespace HealthcareTransparencyParser
{
    /// <summary>
    /// Represents a healthcare provider
    /// </summary>
    public class Provider
    {
        public string ProviderId { get; set; }
        public string NPI { get; set; }
        public string TIN_Type { get; set; }
        public string TIN_Value { get; set; }
        public string Entity_Type { get; set; }
        public string Organization_Name { get; set; }
        public string PrimaryFirstName { get; set; }
        public string PrimaryMiddleName { get; set; }
        public string PrimaryLastName { get; set; }
        public string PrimarySuffix { get; set; }
        public List<ProviderAddress> Addresses { get; set; }
    }

    /// <summary>
    /// Represents a provider address
    /// </summary>
    public class ProviderAddress
    {
        public Guid AddressId { get; set; }
        public string ProviderId { get; set; }
        public string AddressType { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string ZipCode { get; set; }
    }

    /// <summary>
    /// Represents the processing state
    /// </summary>
    public class ProcessingState
    {
        public Guid Id { get; set; }
        public string Url { get; set; }
        public string SchemaType { get; set; }
        public string LastProcessedId { get; set; }
        public bool IsCompleted { get; set; }
        public DateTime StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
    }
}