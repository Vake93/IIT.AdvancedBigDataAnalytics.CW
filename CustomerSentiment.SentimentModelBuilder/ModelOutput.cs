using Microsoft.ML.Data;

namespace CustomerSentiment.SentimentModelBuilder
{
    public class ModelOutput
    {
        [ColumnName("PredictedLabel")]
        public bool Prediction { get; set; }

        public float Score { get; set; }
    }
}
