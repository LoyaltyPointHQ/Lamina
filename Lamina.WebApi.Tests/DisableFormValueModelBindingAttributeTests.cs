using Lamina.WebApi.Controllers.Attributes;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Abstractions;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using Microsoft.AspNetCore.Routing;
using Moq;

namespace Lamina.WebApi.Tests;

public class DisableFormValueModelBindingAttributeTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RemovesOnlyFormFactoriesWithoutReadingBody(bool includeFormFactories)
    {
        var body = new Mock<Stream>(MockBehavior.Strict);
        var httpContext = new DefaultHttpContext();
        httpContext.Request.Body = body.Object;
        httpContext.Request.ContentType = "multipart/form-data; boundary=lamina-test";
        var actionContext = new ActionContext(httpContext, new RouteData(), new ActionDescriptor());
        var route = new RouteValueProviderFactory();
        var query = new QueryStringValueProviderFactory();
        var custom = new Mock<IValueProviderFactory>(MockBehavior.Strict).Object;
        var factories = new List<IValueProviderFactory> { route, query, custom };
        if (includeFormFactories)
        {
            factories.Insert(0, new FormValueProviderFactory());
            factories.Insert(2, new FormFileValueProviderFactory());
            factories.Add(new JQueryFormValueProviderFactory());
        }
        var filters = new List<IFilterMetadata>();
        var executing = new ResourceExecutingContext(actionContext, filters, factories);
        var filter = new DisableFormValueModelBindingAttribute();

        filter.OnResourceExecuting(executing);
        filter.OnResourceExecuted(new ResourceExecutedContext(actionContext, filters));

        Assert.Equal(new IValueProviderFactory[] { route, query, custom }, executing.ValueProviderFactories);
        body.VerifyNoOtherCalls();
        Assert.Same(body.Object, httpContext.Request.Body);
        Assert.Equal("multipart/form-data; boundary=lamina-test", httpContext.Request.ContentType);
    }
}
