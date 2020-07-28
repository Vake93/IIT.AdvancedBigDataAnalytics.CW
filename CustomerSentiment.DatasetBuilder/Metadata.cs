using System.Text.Json.Serialization;

namespace CustomerSentiment.DatasetBuilder
{
    public class Metadata
    {
        [JsonPropertyName("asin")]
        public string Id { get; set; }

        [JsonPropertyName("title")]
        public string Name { get; set; }

        [JsonPropertyName("brand")]
        public string Brand { get; set; }

        [JsonPropertyName("description")]
        public string[] Description { get; set; }

        [JsonPropertyName("main_cat")]
        public string MainCategory { get; set; }

        [JsonPropertyName("category")]
        public string[] Categories { get; set; }

        [JsonPropertyName("image")]
        public string[] ImageUrls { get; set; }

        [JsonPropertyName("price")]
        public string Price { get; set; }

        [JsonPropertyName("rank")]
        public string Rank { get; set; }

        [JsonPropertyName("date")]
        public string Date { get; set; }
    }
}
