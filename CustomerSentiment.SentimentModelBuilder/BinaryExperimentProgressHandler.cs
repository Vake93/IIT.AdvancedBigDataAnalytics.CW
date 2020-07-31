using Microsoft.ML.AutoML;
using Microsoft.ML.Data;
using System;

namespace CustomerSentiment.SentimentModelBuilder
{
    public class BinaryExperimentProgressHandler : IProgress<RunDetail<BinaryClassificationMetrics>>
    {
        private int iterationIndex;

        public void Report(RunDetail<BinaryClassificationMetrics> iterationResult)
        {
            if (iterationIndex == 0)
            {
                PrintBinaryClassificationMetricsHeader();
            }

            if (iterationResult.Exception is null)
            {
                PrintIterationMetrics(
                    iterationIndex,
                    iterationResult.TrainerName,
                    iterationResult.ValidationMetrics,
                    iterationResult.RuntimeInSeconds);
            }
            else
            {
                PrintIterationException(iterationResult.Exception);
            }

            iterationIndex += 1;
        }

        private static void PrintBinaryClassificationMetricsHeader()
        {
            Console.WriteLine($" {"",-4} {"Trainer",-35} {"Accuracy",9} {"AUC",8} {"AUPRC",8} {"F1-score",9} {"Duration",9}");
        }

        private static void PrintIterationMetrics(int iteration, string trainerName, BinaryClassificationMetrics metrics, double? runtimeInSeconds)
        {
            Console.WriteLine($" {iteration,-4} {trainerName,-35} {metrics?.Accuracy ?? double.NaN,9:F4} {metrics?.AreaUnderRocCurve ?? double.NaN,8:F4} {metrics?.AreaUnderPrecisionRecallCurve ?? double.NaN,8:F4} {metrics?.F1Score ?? double.NaN,9:F4} {runtimeInSeconds.Value,9:F1}");
        }

        private static void PrintIterationException(Exception ex)
        {
            Console.WriteLine($"Exception during AutoML iteration: {ex}");
        }
    }
}
