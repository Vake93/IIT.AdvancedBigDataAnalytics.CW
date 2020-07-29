using Microsoft.ML;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using static Microsoft.Spark.Sql.Functions;

namespace CustomerSentiment.Spark
{
    public class Program
    {
        private static readonly MLContext _mlContext = new MLContext();
        private static readonly ITransformer _mlModel = _mlContext
                .Model
                .Load(@"SentimentModel.zip", out var _);

        static void Main()
        {
            const string appName = nameof(CustomerSentiment);
            const string metadataPath = @"hdfs://localhost:9000/data/Electronics_Metadata.json";
            const string reviewsPath = @"hdfs://localhost:9000/data/Electronics_Reviews.json";

            var spark = SparkSession
                .Builder()
                .AppName(appName)
                .GetOrCreate();

            var start = DateTime.Now;

            var dfMetadata = LoadMetadataFile(metadataPath, spark);
            MetadataCleanup(dfMetadata);

            var dfReviews = LoadReviewPathFile(reviewsPath, spark);
            ReviewsCleanup(dfReviews);

            ElectronicsReviewsSentimentAnalysis(spark);

            AnalyseCategorySentiment(spark);

            AnalyseBrandSentiment(spark);

            AnalyseProductSentiment(spark);

            var end = DateTime.Now;
            Console.WriteLine($"Time Elapsed : {(end - start).TotalSeconds} Seconds");
        }

        private static DataFrame LoadMetadataFile(string metadataPath, SparkSession spark)
        {
            Console.WriteLine("Loading JSON File");

            var metadataSchema = new StructType(new[]
            {
                new StructField("asin", new StringType(), isNullable: false),
                new StructField("title", new StringType()),
                new StructField("brand", new StringType()),
                new StructField("main_cat", new StringType()),
                new StructField("price", new StringType()),
                new StructField("category", new ArrayType(new StringType())),
                new StructField("description", new StringType()),
                new StructField("image", new ArrayType(new StringType())),
                new StructField("date", new StringType()),
                new StructField("also_buy", new ArrayType(new StringType())),
                new StructField("also_view", new ArrayType(new StringType())),
                new StructField("rank", new StringType()),
            });

            var dfMetadata = spark
                .Read()
                .Schema(metadataSchema)
                .Json(metadataPath);

            Console.WriteLine("Done");
            Console.WriteLine();

            return dfMetadata;
        }

        private static void MetadataCleanup(DataFrame dataFrame)
        {
            Console.WriteLine("Metadata Clean-up");

            var priceCleanup = Udf<string, float>(
                p =>
                {
                    if (!string.IsNullOrEmpty(p))
                    {
                        var index = 0;

                        for (var i = 0; i < p.Length; i++)
                        {
                            if (char.IsDigit(p[i]))
                            {
                                index = i;
                                break;
                            }
                        }

                        if (float.TryParse(p.Substring(index), out var result))
                        {
                            return result;
                        }
                    }

                    return -1f;
                });

            var dateCleanup = Udf<string, double>(
                d =>
                {
                    if (!string.IsNullOrEmpty(d) && DateTime.TryParse(d, out var result))
                    {
                        return (result.ToUniversalTime() - new DateTime(1970, 1, 1)).TotalSeconds;
                    }

                    return -1L;
                });

            var rankCleanup = Udf<string, long>(
                r =>
                {
                    if (!string.IsNullOrEmpty(r))
                    {
                        var regex = new Regex(@"\d+(,\d+)*", RegexOptions.Singleline);
                        var match = regex.Match(r);
                        if (match.Success && long.TryParse(match.Value.Replace(",", string.Empty), out var result))
                        {
                            return result;
                        }
                    }

                    return -1L;
                });

            dataFrame = dataFrame
                .Filter(
                    dataFrame["asin"].IsNotNull()
                    .And(dataFrame["title"].IsNotNull())
                    .And(dataFrame["main_cat"].IsNotNull())
                    .And(dataFrame["brand"].IsNotNull())
                    .And(Not(dataFrame["main_cat"].IsIn("Grocery", "Pet Supplies", "Baby", "Books", "Appstore for Android", "Gift Cards"))));

            dataFrame = dataFrame
                .WithColumn("clean_price", priceCleanup(dataFrame["price"]))
                .WithColumn("clean-date", dateCleanup(dataFrame["date"]))
                .WithColumn("clean-rank", rankCleanup(dataFrame["rank"]))
                .Drop(dataFrame["price"])
                .Drop(dataFrame["date"])
                .Drop(dataFrame["rank"])
                .WithColumnRenamed("clean_price", "price")
                .WithColumnRenamed("clean-date", "unixTime")
                .WithColumnRenamed("clean-rank", "rank");

            dataFrame.Cache();
            dataFrame.CreateOrReplaceTempView("ElectronicsMetadata");

            Console.WriteLine($"Metadata Count: {dataFrame.Count()}");
            Console.WriteLine("Done");
            Console.WriteLine();
        }

        private static DataFrame LoadReviewPathFile(string reviewPath, SparkSession spark)
        {
            Console.WriteLine("Loading JSON File");

            var ratingSchema = new StructType(new[]
            {
                new StructField("reviewerID", new StringType(), isNullable: false),
                new StructField("asin", new StringType(), isNullable: false),
                new StructField("reviewText", new StringType()),
                new StructField("summary", new StringType()),
                new StructField("overall", new FloatType()),
                new StructField("unixReviewTime", new LongType())
            });

            var dfRatings = spark
                .Read()
                .Schema(ratingSchema)
                .Json(reviewPath);

            var itemIds = spark.Sql(
                "SELECT asin AS id " +
                "FROM ElectronicsMetadata");

            var avaliableItemReviws = dfRatings
                .Join(itemIds, dfRatings["asin"] == itemIds["id"])
                .Drop("id");

            Console.WriteLine("Done");
            Console.WriteLine();

            return avaliableItemReviws;
        }

        private static void ReviewsCleanup(DataFrame dataFrame)
        {
            Console.WriteLine("Ratings Clean-up");

            dataFrame = dataFrame
                .Filter(
                    dataFrame["reviewerID"].IsNotNull()
                    .And(dataFrame["asin"].IsNotNull())
                    .And(dataFrame["reviewText"].IsNotNull())
                    .And(dataFrame["overall"].IsNotNull()));

            dataFrame = dataFrame
                .WithColumnRenamed("reviewerID", "rid")
                .WithColumnRenamed("reviewText", "review_text")
                .WithColumnRenamed("unixReviewTime", "unixTime");

            dataFrame.Cache();

            dataFrame.CreateOrReplaceTempView("ElectronicsReviews");

            Console.WriteLine($"Reviews Count: {dataFrame.Count()}");
            Console.WriteLine("Done");
            Console.WriteLine();
        }

        private static void CreateRatingDataset(SparkSession spark)
        {
            const string datasetFilePath = @"D:\Projects\IIT.AdvancedBigDataAnalytics.CW\Dataset\RatingSentiment.tsv";

            Console.WriteLine("Creating Traning Dataset");
            var reviews = spark.Sql(
                "SELECT review_text, (CASE WHEN overall >= 3 THEN 1 ELSE 0 END) AS positive_review " +
                "FROM ElectronicsRatings " +
                "WHERE LENGTH(review_text) >= 5");

            reviews.Cache();

            var positiveReviews = reviews
                .Filter(reviews["positive_review"] == 1)
                .Take(500)
                .ToArray();

            var negativeReviews = reviews
                .Filter(reviews["positive_review"] == 0)
                .Take(500)
                .ToArray();

            if (File.Exists(datasetFilePath))
            {
                File.Delete(datasetFilePath);
            }

            using var writter = new StreamWriter(datasetFilePath);
            writter.WriteLine("Text\tSentiment");

            for (var i = 0; i < 500; i++)
            {
                var positiveReview = positiveReviews[i];
                var negativeReview = negativeReviews[i];

                var positiveReviewText = positiveReview.GetAs<string>(0);
                var negativeReviewText = negativeReview.GetAs<string>(0);

                writter.WriteLine($"{positiveReviewText}\t{positiveReview[1]}");
                writter.WriteLine($"{negativeReviewText}\t{negativeReview[1]}");
            }

            Console.WriteLine("Done");
        }

        private static int Sentiment(string text)
        {
            var predEngine = _mlContext
                .Model
                .CreatePredictionEngine<ModelInput, ModelOutput>(_mlModel);

            var result = predEngine.Predict(
                new ModelInput { Text = text });

            return result.Prediction == "1" ? 1 : 0;
        }

        private static void ElectronicsReviewsSentimentAnalysis(SparkSession spark)
        {
            spark.Udf().Register<string, int>("sentiment_udf", text => Sentiment(text));

            var reviewsSentiment = spark.Sql("SELECT *, sentiment_udf(review_text) AS sentiment FROM ElectronicsReviews");
            // var reviewsSentiment = spark.Sql("SELECT *, (CASE WHEN overall >= 3 THEN 1 ELSE 0 END) AS sentiment FROM ElectronicsReviews");

            reviewsSentiment.Cache();
            reviewsSentiment.CreateOrReplaceTempView("ElectronicsReviewSentiment");
        }

        private static void AnalyseCategorySentiment(SparkSession spark)
        {
            Console.WriteLine("Analysing category consumer sentiment");

            var itemCategorySentiment = spark.Sql(
                "SELECT EM.main_cat, SUM(ERS.sentiment) / COUNT(1) * 100 as sentiment_rank, COUNT(1) review_count " +
                "FROM ElectronicsMetadata EM " +
                "JOIN ElectronicsReviewSentiment ERS ON ERS.asin = EM.asin " +
                "GROUP BY EM.main_cat");

            itemCategorySentiment.Cache();
            itemCategorySentiment.CreateOrReplaceTempView("ItemCategorySentiment");

            Console.WriteLine("Analysing top 20 categories with best consumer sentiment.");

            var bestCategorySentiment = spark.Sql(
                "SELECT * " +
                "FROM ItemCategorySentiment " +
                "ORDER BY sentiment_rank DESC, review_count DESC " +
                "LIMIT 20");

            bestCategorySentiment.Show();

            Console.WriteLine("Analysing top 20 categories with worst consumer sentiment.");

            var worstCategorySentiment = spark.Sql(
                "SELECT * " +
                "FROM ItemCategorySentiment " +
                "ORDER BY sentiment_rank ASC, review_count DESC " +
                "LIMIT 20");

            worstCategorySentiment.Show();
        }

        private static void AnalyseBrandSentiment(SparkSession spark)
        {
            Console.WriteLine("Analysing brand consumer sentiment");

            var brandSentiment = spark.Sql(
                "SELECT EM.brand, SUM(ERS.sentiment) / COUNT(1) * 100 as sentiment_rank, COUNT(1) review_count " +
                "FROM ElectronicsMetadata EM " +
                "JOIN ElectronicsReviewSentiment ERS ON ERS.asin = EM.asin " +
                "GROUP BY EM.brand " +
                "HAVING COUNT(1) >= 2000");

            brandSentiment.Cache();
            brandSentiment.CreateOrReplaceTempView("BrandSentiment");

            Console.WriteLine("Analysing top 20 most popular brands.");

            var mostPopularBrands = spark.Sql(
                "SELECT * " +
                "FROM BrandSentiment " +
                "ORDER BY review_count DESC " +
                "LIMIT 20");

            mostPopularBrands.Show();

            Console.WriteLine("Analysing top 20 brands with best consumer sentiment.");

            var bestBrandSentiment = spark.Sql(
                "SELECT * " +
                "FROM BrandSentiment " +
                "ORDER BY sentiment_rank DESC, review_count DESC " +
                "LIMIT 20");

            bestBrandSentiment.Show();

            Console.WriteLine("Analysing top 20 lest popular brands.");

            var leastPopularBrands = spark.Sql(
                "SELECT * " +
                "FROM BrandSentiment " +
                "ORDER BY review_count ASC " +
                "LIMIT 20");

            leastPopularBrands.Show();

            Console.WriteLine("Analysing top 20 brands with worst consumer sentiment.");

            var worstBrandSentiment = spark.Sql(
                "SELECT * " +
                "FROM BrandSentiment " +
                "ORDER BY sentiment_rank ASC, review_count ASC " +
                "LIMIT 20");

            worstBrandSentiment.Show();
        }

        private static void AnalyseProductSentiment(SparkSession spark)
        {
            Console.WriteLine("Analysing product consumer sentiment");

            var productSentiment = spark.Sql(
                "SELECT EM.title, EM.brand,  SUM(ERS.sentiment) / COUNT(1) * 100 as sentiment_rank, COUNT(1) review_count " +
                "FROM ElectronicsMetadata EM " +
                "JOIN ElectronicsReviewSentiment ERS ON ERS.asin = EM.asin " +
                "GROUP BY EM.title, EM.brand " +
                "HAVING COUNT(1) >= 100");

            productSentiment.Cache();
            productSentiment.CreateOrReplaceTempView("ProductSentiment");

            Console.WriteLine("Analysing top 20 most popular products.");

            var mostPopularProducts = spark.Sql(
                "SELECT * " +
                "FROM ProductSentiment " +
                "ORDER BY review_count DESC " +
                "LIMIT 20");

            mostPopularProducts.Show();

            Console.WriteLine("Analysing top 20 products with best consumer sentiment.");

            var bestproductSentiment = spark.Sql(
                "SELECT * " +
                "FROM ProductSentiment " +
                "ORDER BY sentiment_rank DESC, review_count DESC " +
                "LIMIT 20");

            bestproductSentiment.Show();

            Console.WriteLine("Analysing top 20 lest popular products.");

            var lestPopularProducts = spark.Sql(
                "SELECT * " +
                "FROM ProductSentiment " +
                "ORDER BY review_count ASC " +
                "LIMIT 20");

            lestPopularProducts.Show();

            Console.WriteLine("Analysing top 20 products with worst consumer sentiment.");

            var worstproductSentiment = spark.Sql(
                "SELECT * " +
                "FROM ProductSentiment " +
                "ORDER BY sentiment_rank ASC, review_count ASC " +
                "LIMIT 20");

            worstproductSentiment.Show();
        }
    }
}
