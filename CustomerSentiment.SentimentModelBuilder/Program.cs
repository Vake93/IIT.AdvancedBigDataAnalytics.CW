using Microsoft.ML;
using Microsoft.ML.AutoML;
using Microsoft.ML.Data;
using System;
using System.IO;
using System.Linq;

namespace CustomerSentiment.SentimentModelBuilder
{
    public class Program
    {
        private const string TraningDataPath = @"D:\Projects\IIT.AdvancedBigDataAnalytics.CW\Dataset\ReviewSentiment.tsv";
        private const string ModelPath = @"D:\Projects\IIT.AdvancedBigDataAnalytics.CW\Model\SentimentModel.zip";

        public static void Main()
        {
            var mlContext = new MLContext(seed: 1024);

            var trainData = mlContext
                .Data
                .LoadFromTextFile<ModelInput>(
                    TraningDataPath,
                    hasHeader: false,
                    separatorChar: '\t',
                    allowQuoting: true,
                    trimWhitespace: true);

            var experimentSettings = new BinaryExperimentSettings
            {
                MaxExperimentTimeInSeconds = (uint)TimeSpan.FromMinutes(5).TotalSeconds,
                OptimizingMetric = BinaryClassificationMetric.Accuracy,
            };

            var experiment = mlContext.Auto().CreateBinaryClassificationExperiment(experimentSettings);

            var preFeaturizer = mlContext.Transforms.Text.FeaturizeText("FeaturizeText", "text")
                .Append(mlContext.Transforms.NormalizeMinMax("Features", "FeaturizeText"));

            var experimentResult = experiment.Execute(
                trainData,
                "sentiment",
                preFeaturizer: preFeaturizer,
                progressHandler: new BinaryExperimentProgressHandler());

            var bestRun = experimentResult.BestRun;

            PrintMetrics(bestRun.TrainerName, bestRun.ValidationMetrics);

            if (File.Exists(ModelPath))
            {
                File.Delete(ModelPath);
            }

            mlContext.Model.Save(bestRun.Model, trainData.Schema, ModelPath);
        }

        private static void PrintMetrics(string name, BinaryClassificationMetrics metrics)
        {
            Console.WriteLine();
            Console.WriteLine($" Metrics for {name} binary classification model");
            Console.WriteLine();
            Console.WriteLine($" Accuracy                          :  {metrics.Accuracy:P2}");
            Console.WriteLine($" Area Under Curve                  :  {metrics.AreaUnderRocCurve:P2}");
            Console.WriteLine($" Area under Precision recall Curve :  {metrics.AreaUnderPrecisionRecallCurve:P2}");
            Console.WriteLine($" F1Score                           :  {metrics.F1Score:P2}");
            Console.WriteLine($" PositivePrecision                 :  {metrics.PositivePrecision:#.##}");
            Console.WriteLine($" PositiveRecall                    :  {metrics.PositiveRecall:#.##}");
            Console.WriteLine($" NegativePrecision                 :  {metrics.NegativePrecision:#.##}");
            Console.WriteLine($" NegativeRecall                    :  {metrics.NegativeRecall:P2}");
            Console.WriteLine();
        }
    }
}
