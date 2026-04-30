using Lamina.Core.Models;
using Lamina.WebApi.Helpers;

namespace Lamina.WebApi.Tests;

public class S3XmlDeserializerTests
{
    private const string S3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/";

    [Fact]
    public void Deserialize_WithNamespace_Succeeds()
    {
        var xml = $"""
            <Delete xmlns="{S3Namespace}">
              <Object><Key>file.txt</Key></Object>
              <Quiet>true</Quiet>
            </Delete>
            """;

        var result = S3XmlDeserializer.Deserialize<DeleteMultipleObjectsRequest, DeleteMultipleObjectsRequestNoNamespace>(
            xml,
            noNs => new DeleteMultipleObjectsRequest { Objects = noNs.Objects, Quiet = noNs.Quiet });

        Assert.True(result.IsSuccess);
        Assert.Single(result.Value!.Objects);
        Assert.Equal("file.txt", result.Value!.Objects[0].Key);
        Assert.True(result.Value!.Quiet);
    }

    [Fact]
    public void Deserialize_WithoutNamespace_Succeeds()
    {
        var xml = """
            <Delete>
              <Object><Key>file.txt</Key></Object>
              <Quiet>false</Quiet>
            </Delete>
            """;

        var result = S3XmlDeserializer.Deserialize<DeleteMultipleObjectsRequest, DeleteMultipleObjectsRequestNoNamespace>(
            xml,
            noNs => new DeleteMultipleObjectsRequest { Objects = noNs.Objects, Quiet = noNs.Quiet });

        Assert.True(result.IsSuccess);
        Assert.Single(result.Value!.Objects);
        Assert.Equal("file.txt", result.Value!.Objects[0].Key);
        Assert.False(result.Value!.Quiet);
    }

    [Fact]
    public void Deserialize_MalformedXml_ReturnsError()
    {
        var xml = "this is not xml <<<";

        var result = S3XmlDeserializer.Deserialize<DeleteMultipleObjectsRequest, DeleteMultipleObjectsRequestNoNamespace>(
            xml,
            noNs => new DeleteMultipleObjectsRequest { Objects = noNs.Objects, Quiet = noNs.Quiet });

        Assert.False(result.IsSuccess);
        Assert.NotNull(result.ErrorMessage);
    }

    [Fact]
    public void Deserialize_WrongRootElement_ReturnsError()
    {
        var xml = "<WrongRoot><Object><Key>file.txt</Key></Object></WrongRoot>";

        var result = S3XmlDeserializer.Deserialize<DeleteMultipleObjectsRequest, DeleteMultipleObjectsRequestNoNamespace>(
            xml,
            noNs => new DeleteMultipleObjectsRequest { Objects = noNs.Objects, Quiet = noNs.Quiet });

        Assert.False(result.IsSuccess);
    }

    [Fact]
    public void Deserialize_MultipleObjects_PreservesAll()
    {
        var xml = """
            <Delete>
              <Object><Key>a.txt</Key></Object>
              <Object><Key>b.txt</Key></Object>
              <Object><Key>c.txt</Key></Object>
            </Delete>
            """;

        var result = S3XmlDeserializer.Deserialize<DeleteMultipleObjectsRequest, DeleteMultipleObjectsRequestNoNamespace>(
            xml,
            noNs => new DeleteMultipleObjectsRequest { Objects = noNs.Objects, Quiet = noNs.Quiet });

        Assert.True(result.IsSuccess);
        Assert.Equal(3, result.Value!.Objects.Count);
    }
}
