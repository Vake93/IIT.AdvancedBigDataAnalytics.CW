using Microsoft.ML.Data;

namespace CustomerSentiment.SentimentModelBuilder
{
    public class ModelOutput
    {
        [ColumnName("PredictedLabel")]
        public string Prediction { get; set; }

        public float[] Score { get; set; }
    }
}
