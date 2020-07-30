﻿using System;

namespace CustomerSentiment.Spark.Models
{
    public class BrandSentiment
    {
        public string Brand { get; set; }

        public double SentimentRank { get; set; }

        public int ReviewCount { get; set; }
    }
}
