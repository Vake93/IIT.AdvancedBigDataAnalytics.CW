using System;

namespace CustomerSentiment.Spark.Models
{
    public class BrandSentiment
    {
        public Guid Id { get; set; }

        public string Brand { get; set; }

        public double SentimentRank { get; set; }

        public int ReviewCount { get; set; }
    }
}
