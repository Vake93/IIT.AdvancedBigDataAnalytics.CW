using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

namespace CustomerSentiment.DatasetBuilder
{
    public class Program
    {
        private const string MetadataFile = @"D:\Projects\IIT.AdvancedBigDataAnalytics.CW\Dataset\Electronics_Metadata.json";
        private const string MetadataDevFile = @"D:\Projects\IIT.AdvancedBigDataAnalytics.CW\Dataset\Electronics_Metadata_Dev.json";

        private const string ReviewsFile = @"D:\Projects\IIT.AdvancedBigDataAnalytics.CW\Dataset\Electronics_Review.json";
        private const string ReviewsDevFile = @"D:\Projects\IIT.AdvancedBigDataAnalytics.CW\Dataset\Electronics_Review_Dev.json";

        public static void Main()
        {
            CreateSampleDatasetAsync<Metadata>(MetadataFile, MetadataDevFile, 1000, false).Wait();
            CreateSampleDatasetAsync<Review>(ReviewsFile, ReviewsDevFile, 1000, true).Wait();
        }

        public static async Task CreateSampleDatasetAsync<T>(string sourcePath, string samplePath, int sampleSize, bool clean)
            where T : class
        {
            if (File.Exists(samplePath))
            {
                File.Delete(samplePath);
            }

            using var reader = new StreamReader(sourcePath);
            using var writer = new StreamWriter(samplePath);

            for (var i = 0; i < sampleSize; i++)
            {
                var line = await reader.ReadLineAsync();

                if (string.IsNullOrEmpty(line))
                {
                    continue;
                }

                if (clean)
                {
                    var data = JsonSerializer.Deserialize<T>(line);
                    await writer.WriteLineAsync(JsonSerializer.Serialize(data));
                }
                else
                {
                    await writer.WriteLineAsync(line);
                }
            }
        }
    }
}
