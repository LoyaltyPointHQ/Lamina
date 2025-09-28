using Microsoft.AspNetCore.Mvc.Abstractions;
using Microsoft.AspNetCore.Mvc.ActionConstraints;

namespace Lamina.WebApi.Controllers.Attributes;

public class RequireQueryParameterAttribute : ActionMethodSelectorAttribute
{
    private readonly string[] _parameterNames;

    public RequireQueryParameterAttribute(params string[] parameterNames)
    {
        _parameterNames = parameterNames ?? throw new ArgumentNullException(nameof(parameterNames));
    }

    public override bool IsValidForRequest(RouteContext routeContext, ActionDescriptor action)
    {
        var query = routeContext.HttpContext.Request.Query;
        return _parameterNames.All(p => query.ContainsKey(p));
    }
}