using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using Google.Protobuf.Collections;
using Microsoft.IdentityModel.Tokens;
using Yandex.Cloud;
using Yandex.Cloud.Credentials;
using Yandex.Cloud.Iam.V1;
using Yandex.Cloud.Compute.V1;

namespace Example
{
    public static class Program
    {
        // Local variables
        private static string configPath = "config.json";
        private static string publicKeyPathEnvVar = "SSH_PUBLIC_KEY_PATH";
        private static string authKeyEnvVar = "AUTH_KEY";
        public static async Task Main(string[] args)
        {
            // Auth with IAM JWT credentials and create Compute instance
            await WithIamJwtCredentials(async credProvider =>
            {
                var sdk = new Sdk(credProvider);
                var sshPublicKey = await ReadSshPublicKey();
                var config = await ReadComputeConfig();

                if (config == null)
                {
                    Console.WriteLine("Configuration could not be read.");
                    return;
                }

                var createInstanceRequest = await CreateInstanceRequest(config, sshPublicKey, sdk);
                var computeResponse = await sdk.Services.Compute.InstanceService.CreateAsync(createInstanceRequest).ResponseAsync;

                Console.WriteLine($"Running Yandex.Cloud operation. ID: {computeResponse.Id}");
            });
        }


        // Prepare instance request object
        private static async Task<CreateInstanceRequest> CreateInstanceRequest(InstanceConfig config, string sshPublicKey, Sdk sdk)
        {
            var resourceSpec = new Yandex.Cloud.Compute.V1.ResourcesSpec
            {
                Cores = config.Resources.ResourcesSpec.Cores,
                Memory = config.Resources.ResourcesSpec.Memory
            };

            var imageId = await GetImageId(sdk, config);
            
            var diskSpec = new AttachedDiskSpec.Types.DiskSpec
            {
                Size = config.Resources.BootDiskSpec.DiskSpec.Size,
                ImageId = imageId,
                TypeId = config.Resources.BootDiskSpec.DiskSpec.TypeId
            };

            var bootDiskSpec = new AttachedDiskSpec
            {
                AutoDelete = config.Resources.BootDiskSpec.AutoDelete,
                DiskSpec = diskSpec
            };

            var networkInterfaceSpec = new NetworkInterfaceSpec
            {
                SubnetId = config.Resources.SubnetId,
                PrimaryV4AddressSpec = new PrimaryAddressSpec
                {
                    OneToOneNatSpec = new OneToOneNatSpec { IpVersion = IpVersion.Ipv4 }
                }
            };

            var processedMetadata = ReplacePlaceholders(config.Metadata, config.Username, sshPublicKey);

            return new CreateInstanceRequest
            {
                FolderId = config.FolderId,
                Name = config.Resources.Name,
                PlatformId = config.Resources.PlatformId,
                ZoneId = config.Resources.ZoneId,
                ResourcesSpec = resourceSpec,
                BootDiskSpec = bootDiskSpec,
                NetworkInterfaceSpecs = { networkInterfaceSpec },
                Metadata = { processedMetadata },
                Labels = {config.VMLabels}
            };
        }

        // Auth with IAM JWT credentials
        private static async Task WithIamJwtCredentials(Func<IamJwtCredentialsProvider, Task> action)
        {
            var json = Environment.GetEnvironmentVariable(authKeyEnvVar);
            if (json == null)
            {
                throw new InvalidOperationException($"{authKeyEnvVar} environment variable is not set");
            };
            var container = JsonSerializer.Deserialize<JsonContainer>(json);
            if (container == null)
            {
                throw new InvalidOperationException("Invalid JSON");
            }

            var rsa = RSA.Create();
            rsa.ImportFromPem(container.PrivateKey);
        
            var key = new RsaSecurityKey(rsa)
            {
                KeyId = container.Id
            };

            var credProvider = new IamJwtCredentialsProvider(key, container.ServiceAccountId);
            await action(credProvider);
        }

        // Read config.json file
        private static async Task<InstanceConfig> ReadComputeConfig()
        {
            string json;
            try
            {
                json = await File.ReadAllTextAsync(configPath).ConfigureAwait(false);
                
            }
            catch (Exception ex)
            {
                throw new IOException("Error reading configuration file.", ex);
            }

            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };

            InstanceConfig config = JsonSerializer.Deserialize<InstanceConfig>(json, options);
            if (config == null)
            {
                throw new InvalidOperationException("Deserialization resulted in a null InstanceConfig.");
            }

            MapField<string, string> metadataMap = new MapField<string, string>();
            foreach (var kvp in config.Metadata)
            {
                metadataMap[kvp.Key] = kvp.Value;
            }
            MapField<string, string> labelsMap = new MapField<string, string>();
            foreach (var kvp in config.VMLabels)
            {
                labelsMap[kvp.Key] = kvp.Value;
            }

            return config;
        }

        // Read SSH public key
        private static async Task<String> ReadSshPublicKey()
        {
            var sshPublicKeyPath = Environment.GetEnvironmentVariable(publicKeyPathEnvVar);
            if (sshPublicKeyPath == null)
            {
                throw new InvalidOperationException($"{publicKeyPathEnvVar} environment variable is not set");
            };
            var sshPublicKey = await File.ReadAllTextAsync(sshPublicKeyPath);

            return sshPublicKey;
        }
        
        // Find image id by folderId and familyId
        private static async Task<string> GetImageId(Sdk sdk, InstanceConfig config)
        {
            var request = new GetImageLatestByFamilyRequest
            {
                FolderId = config.Resources.Image.FolderFamilyId,
                Family = config.Resources.Image.Family
            };

            var imageIdResponse = await sdk.Services.Compute.ImageService
                .GetLatestByFamilyAsync(request);

            return imageIdResponse.Id ?? throw new InvalidOperationException("Image not found");
        }

        // Replace metadata placeholders with ssh public key and username
        private static IDictionary<string, string> ReplacePlaceholders(
            IDictionary<string, string> metadata,
            string username,
            string sshPublicKey)
        {
            var keys = new List<string>(metadata.Keys);

            foreach (var key in keys)
            {
                string value = metadata[key];
                if (value != null)
                {
                    value = value.Replace("USERNAME", username);
                    value = value.Replace("SSH_PUBLIC_KEY", sshPublicKey);
                    metadata[key] = value;
                }
            }

            return metadata;
        }
    }

    // Notate JWT token
    class JsonContainer
    {
        [JsonRequired]
        [JsonPropertyName("id")]
        public required string Id { get; init; }

        [JsonRequired]
        [JsonPropertyName("service_account_id")]
        public required string ServiceAccountId { get; init; }

        [JsonPropertyName("created_at")]
        public DateTime CreatedAt { get; init; }

        [JsonPropertyName("key_algorithm")]
        public required string KeyAlgorithm { get; init; }

        [JsonPropertyName("public_key")]
        public required string PublicKey { get; init; }

        [JsonRequired]
        [JsonPropertyName("private_key")]
        public required string PrivateKey { get; init; }
    }

    // Notate config.json file
    class InstanceConfig
    {
        [JsonPropertyName("folder_id")]
        public required string FolderId { get; init; }

        [JsonPropertyName("username")]
        public required string Username { get; init; }

        [JsonPropertyName("resources")]
        public required Resources Resources { get; init; }

        [JsonPropertyName("metadata")]
        public required IDictionary<string, string> Metadata { get; init; } 

        [JsonPropertyName("labels")]
        public required IDictionary<string, string> VMLabels { get; init; } 
    }

    class Resources
    {
        [JsonPropertyName("image")]
        public required Image Image { get; init; }

        [JsonPropertyName("name")]
        public required string Name { get; init; }

        [JsonPropertyName("resources_spec")]
        public required ResourcesSpec ResourcesSpec { get; init; }

        [JsonPropertyName("boot_disk_spec")]
        public required BootDiskSpec BootDiskSpec { get; init; }

        [JsonPropertyName("zone_id")]
        public required string ZoneId { get; init; }

        [JsonPropertyName("platform_id")]
        public required string PlatformId { get; init; }

        [JsonPropertyName("subnet_id")]
        public required string SubnetId { get; init; }
    }

    class Image
    {
        [JsonPropertyName("family")]
        public required string Family { get; init; }

        [JsonPropertyName("folder_family_id")]
        public required string FolderFamilyId { get; init; }
    }

    class ResourcesSpec
    {
        [JsonPropertyName("memory")]
        public long Memory { get; init; }

        [JsonPropertyName("cores")]
        public int Cores { get; init; }
    }

    class BootDiskSpec
    {
        [JsonPropertyName("auto_delete")]
        public bool AutoDelete { get; init; }

        [JsonPropertyName("disk_spec")]
        public required DiskSpec DiskSpec { get; init; }
    }

    class DiskSpec
    {
        [JsonPropertyName("type_id")]
        public required string TypeId { get; init; }

        [JsonPropertyName("size")]
        public long Size { get; init; }
    }

    class VMLabels
    {
        [JsonPropertyName("yc-sdk")]
        public required string NetSDK { get; init; }
    }
}