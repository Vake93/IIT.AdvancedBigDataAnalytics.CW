using CustomerSentiment.Spark.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.ML;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System;
using System.Linq;
using System.Text.RegularExpressions;
using static Microsoft.Spark.Sql.Functions;

namespace CustomerSentiment.Spark
{
    public class Program
    {
        const string _appName = nameof(CustomerSentiment);
        const string _sentimentModelFile = "SentimentModel.zip";
        const string _metadataPath = @"hdfs://localhost:9000/data/Electronics_Metadata.json";
        const string _reviewsPath = @"hdfs://localhost:9000/data/Electronics_Reviews.json";
        const string _connectionString = "Server=localhost;Database=customer-sentiment-db;User Id=postgres;Password=postgres;Application Name=CustomerSentiment;";

        private static readonly MLContext _mlContext = new MLContext();
        private static readonly ITransformer _mlModel = _mlContext
            .Model
            .Load(_sentimentModelFile, out var _);

        private static readonly DbContextOptions<CustomerSentimentContext> _contextOptions = new DbContextOptionsBuilder<CustomerSentimentContext>()
                .UseNpgsql(_connectionString)
                .Options;

        static void Main()
        {
            using var context = new CustomerSentimentContext(_contextOptions);
            context.Database.EnsureCreated();

            var spark = SparkSession
                .Builder()
                .AppName(_appName)
                .GetOrCreate();

            var start = DateTime.Now;

            var dfMetadata = LoadMetadataFile(_metadataPath, spark);
            MetadataCleanup(dfMetadata);

            var dfReviews = LoadReviewPathFile(_reviewsPath, spark);
            ReviewsCleanup(dfReviews);

            ElectronicsReviewsSentimentAnalysis(spark);

            AnalyseCategorySentiment(spark, context);

            AnalyseBrandSentiment(spark, context);

            AnalyseBrandSentimentVsTime(spark, context);

            AnalyseProductSentiment(spark);

            AnalyseCategoryDemand(spark);

            AnalyseBrandDemand(spark);

            var end = DateTime.Now;
            Console.WriteLine($"Time Elapsed : {(end - start).TotalSeconds} Seconds");
        }

        private static DataFrame LoadMetadataFile(string metadataPath, SparkSession spark)
        {
            Console.WriteLine("Loading Electronics_Metadata.json File");

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
            Console.WriteLine("Loading Electronics_Reviews.json File");

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
                .WithColumnRenamed("unixReviewTime", "unix_time");

            dataFrame.Cache();

            dataFrame.CreateOrReplaceTempView("ElectronicsReviews");

            Console.WriteLine($"Reviews Count: {dataFrame.Count()}");
            Console.WriteLine("Done");
            Console.WriteLine();
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

            // var reviewsSentiment = spark.Sql("SELECT *, sentiment_udf(review_text) AS sentiment FROM ElectronicsReviews");
            var reviewsSentiment = spark.Sql("SELECT *, (CASE WHEN overall >= 3 THEN 1 ELSE 0 END) AS sentiment FROM ElectronicsReviews");

            reviewsSentiment.Cache();
            reviewsSentiment.CreateOrReplaceTempView("ElectronicsReviewSentiment");
        }

        private static void AnalyseCategorySentiment(SparkSession spark, CustomerSentimentContext context)
        {
            Console.WriteLine("Analysing category consumer sentiment");

            var itemCategorySentiment = spark.Sql(
                "SELECT EM.main_cat, SUM(ERS.sentiment) / COUNT(1) * 100 as sentiment_rank, COUNT(1) review_count " +
                "FROM ElectronicsMetadata EM " +
                "JOIN ElectronicsReviewSentiment ERS ON ERS.asin = EM.asin " +
                "GROUP BY EM.main_cat");

            itemCategorySentiment.Cache();
            itemCategorySentiment.CreateOrReplaceTempView("ItemCategorySentiment");

            Console.WriteLine("Analysing categories with best consumer sentiment.");

            var categorySentiment = spark.Sql(
                "SELECT * " +
                "FROM ItemCategorySentiment " +
                "ORDER BY sentiment_rank DESC, review_count DESC");

            categorySentiment.Show();
            var categorySentimentItems = categorySentiment
                .Collect()
                .Select(r => new ItemCategorySentiment
                {
                    Id = Guid.NewGuid(),
                    Category = r.GetAs<string>(0),
                    SentimentRank = r.GetAs<double>(1),
                    ReviewCount = r.GetAs<int>(2)
                })
                .ToArray();

            context.ItemCategorySentiment.AddRange(categorySentimentItems);

            context.SaveChanges();
        }

        private static void AnalyseBrandSentiment(SparkSession spark, CustomerSentimentContext context)
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

            var brandSentimentItems = brandSentiment
                .Collect()
                .Select(r => new BrandSentiment
                {
                    Id = Guid.NewGuid(),
                    Brand = r.GetAs<string>(0),
                    SentimentRank = r.GetAs<double>(1),
                    ReviewCount = r.GetAs<int>(2)
                })
                .ToArray();

            context.BrandSentiment.AddRange(brandSentimentItems);
            context.SaveChanges();

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

        private static void AnalyseBrandSentimentVsTime(SparkSession spark, CustomerSentimentContext context)
        {
            Console.WriteLine("Analysing brand consumer sentiment");

            var brandSentimentVsTime = spark.Sql(
                "SELECT EM.brand, FROM_UNIXTIME(ERS.unix_time, 'YYYY') as year, SUM(ERS.sentiment) / COUNT(1) * 100 as sentiment_rank " +
                "FROM ElectronicsMetadata EM " +
                "JOIN ElectronicsReviewSentiment ERS ON ERS.asin = EM.asin " +
                "WHERE EM.brand LIKE 'Amazon%' " +
                "GROUP BY EM.brand, FROM_UNIXTIME(ERS.unix_time, 'YYYY') " +
                "ORDER BY FROM_UNIXTIME(ERS.unix_time, 'YYYY')");

            brandSentimentVsTime.Cache();
            brandSentimentVsTime.CreateOrReplaceTempView("BrandSentimentVsTime");
            brandSentimentVsTime.Show();

            var brandSentimentVsTimeItems = brandSentimentVsTime
                .Collect()
                .Select(r => new BrandSentimentVsTime
                {
                    Id = Guid.NewGuid(),
                    Brand = r.GetAs<string>(0),
                    Year = int.Parse(r.GetAs<string>(1)),
                    SentimentRank = r.GetAs<double>(2)
                })
                .ToArray();

            context.BrandSentimentVsTime.AddRange(brandSentimentVsTimeItems);
            context.SaveChanges();
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

        private static void AnalyseCategoryDemand(SparkSession spark)
        {
            Console.WriteLine("Analysing category consumer demand");

            var categoriesDemand = spark.Sql(
                "SELECT EM.main_cat, FROM_UNIXTIME(ER.unix_time, 'MM') as month, COUNT(1) as demand " +
                "FROM ElectronicsReviews ER " +
                "JOIN ElectronicsMetadata EM ON EM.asin = ER.asin " +
                "GROUP BY EM.main_cat, from_unixtime(ER.unix_time, 'MM') " +
                "ORDER BY EM.main_cat, FROM_UNIXTIME(ER.unix_time, 'MM')");

            categoriesDemand.Cache();
            categoriesDemand.CreateOrReplaceTempView("CategoryDemand");

            var categories = spark.Sql("SELECT main_cat FROM CategoryDemand GROUP BY main_cat")
                .Collect()
                .Select(r => r.GetAs<string>(0))
                .ToArray();

            foreach (var category in categories)
            {
                Console.WriteLine($"Analysing consumer demand for {category}");

                var categoryDemand = spark.Sql(
                    "SELECT * " +
                    "FROM CategoryDemand " +
                   $"WHERE main_cat = '{category}'");

                categoryDemand.Show();
            }
        }

        private static void AnalyseBrandDemand(SparkSession spark)
        {
            Console.WriteLine("Analysing first party brand consumer demand");

            var brandsDemand = spark.Sql(
                "SELECT EM.brand, FROM_UNIXTIME(ER.unix_time, 'MM') as month, COUNT(1) as demand " +
                "FROM ElectronicsReviews ER " +
                "JOIN ElectronicsMetadata EM ON EM.asin = ER.asin " +
                "WHERE EM.brand LIKE 'Amazon%' " +
                "GROUP BY EM.brand, from_unixtime(ER.unix_time, 'MM') " +
                "ORDER BY EM.brand, FROM_UNIXTIME(ER.unix_time, 'MM')");

            brandsDemand.Cache();
            brandsDemand.CreateOrReplaceTempView("BrandsDemand");

            var brands = spark.Sql("SELECT brand FROM BrandsDemand GROUP BY brand")
                .Collect()
                .Select(r => r.GetAs<string>(0))
                .ToArray();

            foreach (var brand in brands)
            {
                Console.WriteLine($"Analysing consumer demand for {brand}");

                var brandDemand = spark.Sql(
                    "SELECT * " +
                    "FROM BrandsDemand " +
                   $"WHERE brand = '{brand}'");

                brandDemand.Show();
            }
        }
    }
}
