# One Dockerfile for all three .NET services. Pick the project with a build arg:
#   docker build -f src/Dockerfile --build-arg PROJECT=BrewQueue.Api .
# Build context must be the repository root (it needs Directory.*.props and global.json).

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG PROJECT
WORKDIR /src

COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/ src/

RUN dotnet restore "src/${PROJECT}/${PROJECT}.csproj"
RUN dotnet publish "src/${PROJECT}/${PROJECT}.csproj" -c Release -o /app --no-restore

# The ASP.NET runtime image also runs plain worker services, so one base image fits all three.
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
ARG PROJECT
ENV APP_DLL=${PROJECT}.dll
WORKDIR /app
COPY --from=build /app .

# Run as the non-root user that the .NET images provide.
USER $APP_UID
ENTRYPOINT ["sh", "-c", "exec dotnet \"$APP_DLL\""]
