using System;

namespace CustomerSentiment.Spark.Models
{
    public class ItemCategorySentiment
    {
        public Guid Id { get; set; }

        public string Category { get; set; }

        public double SentimentRank { get; set; }

        public int ReviewCount { get; set; }
    }
}
