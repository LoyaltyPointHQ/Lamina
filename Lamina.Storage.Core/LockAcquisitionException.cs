namespace Lamina.Storage.Core;

/// <summary>A metadata lock could not be acquired; this does not establish that a resource is missing.</summary>
public sealed class LockAcquisitionException(string operation)
    : Exception($"Unable to acquire the metadata {operation} lock.");
