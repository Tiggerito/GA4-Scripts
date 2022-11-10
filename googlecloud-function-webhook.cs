using CloudNative.CloudEvents;
using Google.Cloud.Functions.Framework;
using Google.Events.Protobuf.Cloud.PubSub.V1;
using System;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

// Runtime: .Net Core 3.1
// Entry point: WebSiteAdvantage.ApiHelpers.GoogleCloudFunctionWebhook
// standard Sample.csproj
// replace Function.cs with this
// modify for your_id and webhook_url

namespace WebSiteAdvantage.ApiHelpers
{
    public class GoogleCloudFunctionWebhook : ICloudEventFunction<MessagePublishedData>
    {
        private static HttpClient? _HttpClient;

        private static HttpClient HttpClient
        {
            get
            {
                if (_HttpClient is null)
                {
                    _HttpClient = new HttpClient();
                }
                return _HttpClient;
            }
        }
        public async Task HandleAsync(CloudEvent cloudEvent, MessagePublishedData data, CancellationToken cancellationToken)
        {
            Console.WriteLine($"CloudEvent type: {cloudEvent.Type}");
            Console.WriteLine($"PubSub message text: {data.Message.TextData}");

            
            var id = "{your_id}";
            var webhook = "{webhook_url}";

            var rn = "unknown";
            JsonElement body = JsonSerializer.Deserialize<JsonElement>(data.Message.TextData);

            if (body.TryGetProperty("protoPayload", out JsonElement protoPayload) && protoPayload.ValueKind == JsonValueKind.Object)
            {
                if (protoPayload.TryGetProperty("resourceName", out JsonElement resourceName) && resourceName.ValueKind == JsonValueKind.String)
                {
                    rn = resourceName.GetString();
                }
            }

            var response = await HttpClient.GetAsync($"{webhook}?id={id}&rn={rn}", cancellationToken);

            Console.WriteLine($"response: {response.StatusCode}");

        }
    }
}
