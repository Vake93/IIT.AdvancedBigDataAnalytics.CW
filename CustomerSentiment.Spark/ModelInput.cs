﻿using Microsoft.ML.Data;

namespace CustomerSentiment.Spark
{
    public class ModelInput
    {
        [ColumnName("text"), LoadColumn(0)]
        public string Text { get; set; }


        [ColumnName("sentiment"), LoadColumn(1)]
        public bool Sentiment { get; set; }
    }
}
