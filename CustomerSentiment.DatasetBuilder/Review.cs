using System.Text.Json.Serialization;

namespace CustomerSentiment.DatasetBuilder
{
    public class Review
    {
        [JsonPropertyName("reviewerID")]
        public string ReviewerId { get; set; }

        [JsonPropertyName("asin")]
        public string Id { get; set; }

        [JsonPropertyName("reviewText")]
        public string ReviewText { get; set; }

        [JsonPropertyName("summary")]
        public string Summary { get; set; }

        [JsonPropertyName("unixReviewTime")]
        public long UnixReviewTime { get; set; }
    }
}
