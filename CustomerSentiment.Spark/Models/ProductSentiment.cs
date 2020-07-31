using System;

namespace CustomerSentiment.Spark.Models
{
    public class ProductSentiment
    {
        public Guid Id { get; set; } = Guid.NewGuid();

        public string Name { get; set; }

        public string Brand { get; set; }

        public double SentimentRank { get; set; }

        public int ReviewCount { get; set; }
    }
}
