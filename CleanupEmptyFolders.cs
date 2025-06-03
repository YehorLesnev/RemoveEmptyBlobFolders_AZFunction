using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Functions.Worker;

namespace RemoveEmptyBlobFolders
{
    public class CleanupEmptyFolders
    {
        private readonly ILogger _logger;
        private readonly BlobServiceClient _blobServiceClient;
        private readonly string _containerName;
        private readonly string _rootPath;
        private readonly DateTimeOffset _thresholdDate;

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

            int retentionMonths = configuration.GetValue<int?>("MinAgeInDaysToDelete") ?? 90;
            _thresholdDate = DateTimeOffset.UtcNow.AddDays(-retentionMonths);
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

            // 1. Delete all blobs (files) under rootPath that are older than 3 months
            await DeleteOldBlobsAtLevel(containerClient, _rootPath);

            // 2. Enumerate immediate subfolders under rootPath and process them recursively
            await foreach (var result in containerClient.GetBlobsByHierarchyAsync(prefix: _rootPath, delimiter: "/"))
            {
                if (!result.IsPrefix)
                    continue;

                string prefix = result.Prefix; // e.g. "rootPath/subfolder1/"
                await ProcessPrefixRecursively(containerClient, prefix);
            }

            _logger.LogInformation("CleanupEmptyFolders completed.");
        }

        /// <summary>
        /// Deletes any blobs directly under the specified prefix (rootPath) whose LastModified is older than threshold.
        /// Only considers blobs at the immediate level (does not recurse into subfolders).
        /// </summary>
        private async Task DeleteOldBlobsAtLevel(BlobContainerClient containerClient, string prefix)
        {
            await foreach (var item in containerClient.GetBlobsByHierarchyAsync(prefix: prefix, delimiter: "/"))
            {
                if (item.IsBlob)
                {
                    var blobItem = item.Blob;

                    // Skip zero-byte folder placeholders (name ends with '/' AND length==0)
                    if (blobItem.Name.EndsWith('/') && blobItem.Properties.ContentLength == 0)
                        continue;

                    // If blob's LastModified is older than threshold, delete it
                    if (blobItem.Properties.LastModified.HasValue &&
                        blobItem.Properties.LastModified.Value < _thresholdDate)
                    {
                        var blobClient = containerClient.GetBlobClient(blobItem.Name);
                        _logger.LogInformation($"Deleting old blob: {blobItem.Name} (LastModified: {blobItem.Properties.LastModified:O})");
                        await blobClient.DeleteIfExistsAsync();
                    }
                }
                // If item.IsPrefix, we skip here—cleanup for subfolders happens in ProcessPrefixRecursively.
            }
        }

        /// <summary>
        /// Recursively processes a virtual folder at 'prefix':
        /// 1. Deletes any blobs in this folder that are older than threshold.
        /// 2. Recurse into any subfolders.
        /// 3. If, after deletion, no real blobs remain in this folder (and no subfolder placeholders),
        ///    attempts to delete the zero-byte placeholder for this folder.
        /// </summary>
        private async Task ProcessPrefixRecursively(BlobContainerClient containerClient, string prefix)
        {
            bool hasAnyChild = false;
            var subPrefixes = new List<string>();
            var blobItems = new List<BlobItem>();

            // 1. List everything under this prefix (one level deep)
            await foreach (var item in containerClient.GetBlobsByHierarchyAsync(prefix: prefix, delimiter: "/"))
            {
                if (item.IsPrefix)
                {
                    subPrefixes.Add(item.Prefix);
                }
                else if (item.IsBlob)
                {
                    blobItems.Add(item.Blob);
                }
            }

            // 2. Process blob items: delete old ones, mark presence of newer blobs
            foreach (var blobItem in blobItems)
            {
                // Skip zero-byte folder placeholders
                if (blobItem.Name.EndsWith('/') && blobItem.Properties.ContentLength == 0)
                    continue;

                // Check LastModified
                if (blobItem.Properties.LastModified.HasValue &&
                    blobItem.Properties.LastModified.Value < _thresholdDate)
                {
                    // Delete old blob
                    var blobClient = containerClient.GetBlobClient(blobItem.Name);
                    _logger.LogInformation($"Deleting old blob: {blobItem.Name} (LastModified: {blobItem.Properties.LastModified:O})");
                    await blobClient.DeleteIfExistsAsync();
                }
                else
                {
                    // Newer blob exists—mark that this folder is not empty
                    hasAnyChild = true;
                }
            }

            // 3. Recurse into each subfolder
            foreach (var subPrefix in subPrefixes)
            {
                await ProcessPrefixRecursively(containerClient, subPrefix);
            }

            // 4. Attempt to delete this folder's placeholder if no real blobs remain
            if (!hasAnyChild)
            {
                // Placeholder might be stored as "prefix/" or without trailing slash.
                string placeholderWithSlash = prefix;                             // e.g. "rootPath/subfolder1/"
                string placeholderWithoutSlash = prefix.TrimEnd('/');             // e.g. "rootPath/subfolder1"

                // Try deleting the trailing-slash placeholder
                var folderBlobClient = containerClient.GetBlobClient(placeholderWithSlash);
                try
                {
                    var properties = await folderBlobClient.GetPropertiesAsync();
                    if (properties.Value.ContentLength == 0)
                    {
                        _logger.LogInformation($"Deleting placeholder folder blob: {placeholderWithSlash}");
                        await folderBlobClient.DeleteIfExistsAsync();
                        return;
                    }
                }
                catch (RequestFailedException ex) when (ex.Status == 404)
                {
                    // Not found with trailing slash—try without slash
                    var altClient = containerClient.GetBlobClient(placeholderWithoutSlash);
                    try
                    {
                        var altProps = await altClient.GetPropertiesAsync();
                        if (altProps.Value.ContentLength == 0)
                        {
                            _logger.LogInformation($"Deleting placeholder folder blob: {placeholderWithoutSlash}");
                            await altClient.DeleteIfExistsAsync();
                        }
                        else
                        {
                            // There's some content here; do not delete
                        }
                    }
                    catch (RequestFailedException ex2) when (ex2.Status == 404)
                    {
                        // No placeholder present at all
                        _logger.LogInformation($"No placeholder blob found for empty folder: {prefix}");
                    }
                }
            }
        }
    }
}