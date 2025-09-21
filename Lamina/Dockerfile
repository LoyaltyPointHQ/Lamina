FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Lamina/Lamina.csproj", "Lamina/"]
RUN dotnet restore "Lamina/Lamina.csproj"
COPY . .
WORKDIR "/src/Lamina"
RUN dotnet build "./Lamina.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./Lamina.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .

# Make the container compatible with OpenShift's arbitrary UID requirement
# while remaining backwards compatible with standard Docker/Kubernetes
RUN chgrp -R 0 /app && \
    chmod -R g=u /app && \
    chmod g+w /app

# Create directories that the app might need to write to
RUN mkdir -p /data /metadata && \
    chgrp -R 0 /data /metadata && \
    chmod -R g=u /data /metadata

# .NET needs a writable home directory for some operations
ENV HOME=/app
ENV DOTNET_CLI_HOME=/app

# Run as non-root user (compatible with both OpenShift and standard deployments)
# OpenShift will override this with an arbitrary UID from the project range
USER 1001

ENTRYPOINT ["dotnet", "Lamina.dll"]
