using System;

namespace CustomerSentiment.Spark.Models
{
    public class BrandSentimentVsTime
    {
        public string Brand { get; set; }

        public int Year { get; set; }

        public double SentimentRank { get; set; }
    }
}
