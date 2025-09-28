using Microsoft.AspNetCore.Mvc.Abstractions;
using Microsoft.AspNetCore.Mvc.ActionConstraints;

namespace Lamina.WebApi.Controllers.Attributes;

public class RequireNoQueryParametersAttribute : ActionMethodSelectorAttribute
{
    private readonly string[] _parameterNames;

    public RequireNoQueryParametersAttribute(params string[] parameterNames)
    {
        _parameterNames = parameterNames ?? throw new ArgumentNullException(nameof(parameterNames));
    }

    public override bool IsValidForRequest(RouteContext routeContext, ActionDescriptor action)
    {
        var query = routeContext.HttpContext.Request.Query;
        return !_parameterNames.Any(p => query.ContainsKey(p));
    }
}