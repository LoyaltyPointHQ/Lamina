using System.Xml.Serialization;
using Lamina.Core.Models;

namespace Lamina.WebApi.Helpers;

public static class S3XmlDeserializer
{
    private const string MalformedXmlMessage =
        "The XML you provided was not well-formed or did not validate against our published schema.";

    public static XmlDeserializationResult<TCanonical> Deserialize<TCanonical, TNoNs>(
        string xml,
        Func<TNoNs, TCanonical> noNsMapper)
    {
        if (TryDeserialize<TCanonical>(xml, out var canonical))
            return XmlDeserializationResult<TCanonical>.Success(canonical);

        if (TryDeserialize<TNoNs>(xml, out var noNs))
            return XmlDeserializationResult<TCanonical>.Success(noNsMapper(noNs));

        return XmlDeserializationResult<TCanonical>.Error(MalformedXmlMessage);
    }

    private static bool TryDeserialize<T>(string xml, out T result)
    {
        result = default!;
        try
        {
            var serializer = new XmlSerializer(typeof(T));
            using var reader = new StringReader(xml);
            var deserialized = serializer.Deserialize(reader);
            if (deserialized is T value)
            {
                result = value;
                return true;
            }
            return false;
        }
        catch
        {
            return false;
        }
    }
}
