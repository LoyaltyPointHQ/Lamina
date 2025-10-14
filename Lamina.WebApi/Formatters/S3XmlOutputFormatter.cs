using System.Text;
using System.Xml;
using System.Xml.Serialization;
using Microsoft.AspNetCore.Mvc.Formatters;

namespace Lamina.WebApi.Formatters;

/// <summary>
/// Custom XML output formatter for S3 API responses.
/// Ensures UTF-8 encoding and suppresses redundant XML namespace declarations (xsi, xsd)
/// to match AWS S3 API specification.
/// </summary>
public class S3XmlOutputFormatter : XmlSerializerOutputFormatter
{
    public S3XmlOutputFormatter(XmlWriterSettings writerSettings) : base(writerSettings)
    {
        // Ensure UTF-8 encoding is used
        writerSettings.Encoding = Encoding.UTF8;
    }

    protected override void Serialize(XmlSerializer xmlSerializer, XmlWriter xmlWriter, object? value)
    {
        // Create XmlSerializerNamespaces to suppress xsi and xsd namespace declarations
        // This ensures clean XML output that matches AWS S3 responses
        var namespaces = new XmlSerializerNamespaces();
        namespaces.Add("", ""); // Add empty prefix for default namespace

        xmlSerializer.Serialize(xmlWriter, value, namespaces);
    }
}
