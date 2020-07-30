using System;

namespace CustomerSentiment.Spark.Models
{
    public class ItemCategorySentiment
    {
        public string Category { get; set; }

        public double SentimentRank { get; set; }

        public int ReviewCount { get; set; }
    }
}
