using Microsoft.ML;
using System;
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

            // Load Data
            var trainingDataView = mlContext.Data.LoadFromTextFile<ModelInput>(
                path: TraningDataPath,
                hasHeader: true,
                separatorChar: '\t',
                allowQuoting: true,
                allowSparse: false);

            // Build training pipeline
            var trainingPipeline = BuildTrainingPipeline(mlContext);

            // Train Model
            var mlModel = TrainModel(trainingDataView, trainingPipeline);

            // Evaluate quality of Model
            Evaluate(mlContext, trainingDataView, trainingPipeline);

            // Save model
            SaveModel(mlContext, mlModel, ModelPath, trainingDataView.Schema);

            // Make test prediction
            TestPrediction(mlContext, mlModel);
        }

        private static IEstimator<ITransformer> BuildTrainingPipeline(MLContext mlContext)
        {
            // Data process configuration with pipeline data transformations 
            var dataProcessPipeline = mlContext.Transforms.Conversion.MapValueToKey("sentiment", "sentiment")
                .Append(mlContext.Transforms.Text.FeaturizeText("featurizeText", "text"))
                .Append(mlContext.Transforms.CopyColumns("Features", "featurizeText"))
                .Append(mlContext.Transforms.NormalizeMinMax("Features", "Features"))
                .AppendCacheCheckpoint(mlContext);

            // Set the training algorithm 
            var binaryEstimator = mlContext.BinaryClassification.Trainers.AveragedPerceptron(
                labelColumnName: "sentiment",
                numberOfIterations: 30,
                featureColumnName: "Features");

            var estimator = mlContext.Transforms.Conversion.MapKeyToValue("PredictedLabel", "PredictedLabel");

            var trainer = mlContext.MulticlassClassification.Trainers.OneVersusAll(binaryEstimator, labelColumnName: "sentiment")
                .Append(estimator);

            return dataProcessPipeline.Append(trainer);
        }

        public static ITransformer TrainModel(IDataView trainingDataView, IEstimator<ITransformer> trainingPipeline)
        {
            Console.WriteLine("Training model");
            Console.WriteLine();
            return trainingPipeline.Fit(trainingDataView);
        }

        private static void Evaluate(MLContext mlContext, IDataView trainingDataView, IEstimator<ITransformer> trainingPipeline)
        {
            Console.WriteLine("Cross-validating model");
            var crossValidationResults = mlContext.MulticlassClassification.CrossValidate(
                trainingDataView,
                trainingPipeline,
                numberOfFolds: 5,
                labelColumnName: "sentiment");

            var confusionMatrix = crossValidationResults
                .Select(r => r.Metrics.ConfusionMatrix)
                .Select(m => new double[,]
                {
                    {m.Counts[0][0], m.Counts[0][1] },
                    {m.Counts[1][0], m.Counts[1][1] },
                })
                .Aggregate((m, n) => new double[,]
                {
                    {m[0,0] + n[0,0], m[0,1] + n[0,1]},
                    {m[1,0] + n[1,0], m[1,1] + n[1,1]}
                });

            Console.WriteLine("ConfusionMatrix: ");
            Console.WriteLine($"{Math.Round(confusionMatrix[0, 0] / 5)}, {Math.Round(confusionMatrix[0, 1] / 5)}");
            Console.WriteLine($"{Math.Round(confusionMatrix[1, 0] / 5)}, {Math.Round(confusionMatrix[1, 1] / 5)}");
            Console.WriteLine();
        }

        private static void SaveModel(MLContext mlContext, ITransformer mlModel, string modelPath, DataViewSchema modelInputSchema)
        {
            Console.WriteLine("Saving the model");
            mlContext.Model.Save(mlModel, modelInputSchema, modelPath);
            Console.WriteLine($"The model is saved to {modelPath}");
        }

        private static void TestPrediction(MLContext mlContext, ITransformer mlModel)
        {
            Console.WriteLine("Test model");
            var predictionEngine = mlContext.Model.CreatePredictionEngine<ModelInput, ModelOutput>(mlModel);
            var result = predictionEngine.Predict(new ModelInput
            {
                Text = "Comfortable, good bass depth and very durable. " +
                "This is my second pair and I have nothing but good things to say!"
            });

            Console.WriteLine($"Predicted {result.Prediction}, scores {string.Join(", ", result.Score)}");
        }
    }
}
