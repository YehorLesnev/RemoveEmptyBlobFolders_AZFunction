using Azure;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Functions.Worker;

namespace RemoveEmptyBlobFolders;

public class CleanupEmptyFolders
{
    private readonly ILogger _logger;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly string _containerName;
    private readonly string _rootPath;

    public CleanupEmptyFolders(IConfiguration configuration, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<CleanupEmptyFolders>();
        
        string storageConnString = configuration.GetValue<string>("AzureWebJobsStorage");

        if (string.IsNullOrEmpty(storageConnString))
			throw new ArgumentException("AzureWebJobsStorage connection string is not configured.");

		_blobServiceClient = new BlobServiceClient(storageConnString);

        _containerName = configuration.GetValue<string>("BlobContainerName", string.Empty).Trim();
        _rootPath = configuration.GetValue<string>("RootBlobPath", string.Empty).Trim();
		if (!string.IsNullOrEmpty(_rootPath) && !_rootPath.EndsWith('/'))
        {
            _rootPath += "/";
        }
    }

    [Function("CleanupEmptyFolders")]
    public async Task Run(
        [TimerTrigger("0 0 0 1 * *", RunOnStartup = false)] TimerInfo timerInfo)
    {
        _logger.LogInformation($"CleanupEmptyFolders triggered at: {DateTime.UtcNow:O}");

        if (string.IsNullOrEmpty(_containerName))
        {
            _logger.LogError("BlobContainerName is not configured.");
            return;
        }

        var containerClient = _blobServiceClient.GetBlobContainerClient(_containerName);
        await foreach (var result in containerClient.GetBlobsByHierarchyAsync(prefix: _rootPath, delimiter: "/"))
        {
            if (!result.IsPrefix)
                return;

            string prefix = result.Prefix;
            await ProcessPrefixRecursively(containerClient, prefix);
        }

        _logger.LogInformation("CleanupEmptyFolders completed.");
    }

    private async Task ProcessPrefixRecursively(BlobContainerClient containerClient, string prefix)
    {
        bool hasAnyChild = false;
        var subPrefixes = new List<string>();

        await foreach (var item in containerClient.GetBlobsByHierarchyAsync(prefix: prefix, delimiter: "/"))
        {
            if (item.IsPrefix)
            {
                subPrefixes.Add(item.Prefix);
            }
            else if (item.IsBlob)
            {
                // If it's a “folder placeholder” blob (zero-length and ends with '/'), skip it for children check
                if (!(item.Blob.Name.EndsWith('/') && item.Blob.Properties.ContentLength == 0))
                {
                    hasAnyChild = true;
                    break;
                }
            }
        }

        // Recurse into subfolders first
        foreach (var subPrefix in subPrefixes)
        {
            await ProcessPrefixRecursively(containerClient, subPrefix);
        }

        if (!hasAnyChild)
        {
            // No real blobs beneath this prefix; attempt to delete a placeholder blob if it exists
            string placeholderBlobName = prefix.TrimEnd('/');

            var blobClient = containerClient.GetBlobClient(prefix);

            try
            {
                var properties = await blobClient.GetPropertiesAsync();

                if (properties.Value.ContentLength != 0)
                    return;

				_logger.LogInformation($"Deleting placeholder folder blob: {prefix}");
                await blobClient.DeleteIfExistsAsync();
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // Placeholder blob with trailing slash not found, try without slash
                var altClient = containerClient.GetBlobClient(placeholderBlobName);

                try
                {
                    var altProps = await altClient.GetPropertiesAsync();
                    if (altProps.Value.ContentLength != 0)
                        return;
                    
                    _logger.LogInformation($"Deleting placeholder folder blob: {placeholderBlobName}");
                    await altClient.DeleteIfExistsAsync();
                }
                catch (RequestFailedException ex2) when (ex2.Status == 404)
                {
                    _logger.LogInformation($"No placeholder blob found for empty folder: {prefix}");
                }
            }
        }
    }
}
