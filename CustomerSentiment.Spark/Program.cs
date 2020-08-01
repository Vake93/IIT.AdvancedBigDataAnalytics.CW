using CustomerSentiment.Spark.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.ML;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System;
using System.Linq;
using System.Text.RegularExpressions;
using static Microsoft.Spark.Sql.Functions;
using CustomerSentimentContextOptions = Microsoft.EntityFrameworkCore.DbContextOptions<CustomerSentiment.Spark.CustomerSentimentContext>;

namespace CustomerSentiment.Spark
{
    public class Program
    {
        const string _appName = nameof(CustomerSentiment);
        const string _sentimentModelFile = "SentimentModel.zip";
        const string _metadataPath = @"hdfs://localhost:9000/data/Electronics_Metadata.json";
        const string _reviewsPath = @"hdfs://localhost:9000/data/Electronics_Reviews.json";
        const string _connectionString = "Server=localhost;Database=customer-sentiment;User Id=postgres;Password=postgres;Application Name=CustomerSentiment;";

        private static readonly MLContext _mlContext;
        private static readonly ITransformer _mlModel;
        private static readonly CustomerSentimentContextOptions _contextOptions;

        static Program()
        {
            _mlContext = new MLContext();

            _mlModel = _mlContext
                .Model
                .Load(_sentimentModelFile, out var _);

            _contextOptions = new DbContextOptionsBuilder<CustomerSentimentContext>()
                .UseNpgsql(_connectionString)
                .Options;
        }

        static void Main()
        {
            using var context = new CustomerSentimentContext(_contextOptions);
            context.Database.EnsureCreated();

            var spark = SparkSession
                .Builder()
                .AppName(_appName)
                .Config("spark.cores.max", "4")
                .Config("spark.ui.port", "4040")
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

            AnalyseProductSentiment(spark, context);

            AnalyseCategoryDemand(spark, context);

            AnalyseBrandDemand(spark, context);

            var end = DateTime.Now;
            Console.WriteLine($"Time Elapsed : {(end - start).TotalMinutes} Minutes");

            Console.ReadLine();
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
                    .And(dataFrame["reviewText"].IsNotNull()));

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

            return result.Prediction ? 1 : 0;
        }

        private static void ElectronicsReviewsSentimentAnalysis(SparkSession spark)
        {
            spark.Udf().Register<string, int>("sentiment_udf", text => Sentiment(text));

            var reviewsSentiment = spark.Sql("SELECT *, sentiment_udf(review_text) AS sentiment FROM ElectronicsReviews");

            reviewsSentiment.Cache();
            reviewsSentiment.CreateOrReplaceTempView("ElectronicsReviewSentiment");
        }

        private static void AnalyseCategorySentiment(SparkSession spark, CustomerSentimentContext context)
        {
            Console.WriteLine("Analyzing category sentiment");

            var itemCategorySentiment = spark.Sql(
                "SELECT EM.main_cat, SUM(ERS.sentiment) / COUNT(1) * 100 as sentiment_rank, COUNT(1) review_count " +
                "FROM ElectronicsMetadata EM " +
                "JOIN ElectronicsReviewSentiment ERS ON ERS.asin = EM.asin " +
                "GROUP BY EM.main_cat");

            itemCategorySentiment.Cache();
            itemCategorySentiment.CreateOrReplaceTempView("ItemCategorySentiment");

            Console.WriteLine("Analyzing categories with best consumer sentiment.");

            var categorySentiment = spark.Sql(
                "SELECT * " +
                "FROM ItemCategorySentiment " +
                "ORDER BY sentiment_rank DESC, review_count DESC");

            categorySentiment.Show();

            var items = Mapper.MapRows(
                categorySentiment.Collect(),
                r => new ItemCategorySentiment
                {
                    Category = r.GetAs<string>(0),
                    SentimentRank = r.GetAs<double>(1),
                    ReviewCount = r.GetAs<int>(2)
                },
                o => o.Category);

            context.ItemCategorySentiment.RemoveRange(context.ItemCategorySentiment);
            context.ItemCategorySentiment.AddRange(items);

            context.SaveChanges();
        }

        private static void AnalyseBrandSentiment(SparkSession spark, CustomerSentimentContext context)
        {
            Console.WriteLine("Analyzing brand consumer sentiment");

            var brandSentiment = spark.Sql(
                "SELECT EM.brand, SUM(ERS.sentiment) / COUNT(1) * 100 as sentiment_rank, COUNT(1) review_count " +
                "FROM ElectronicsMetadata EM " +
                "JOIN ElectronicsReviewSentiment ERS ON ERS.asin = EM.asin " +
                "GROUP BY EM.brand " +
                "HAVING COUNT(1) >= 2000");

            brandSentiment.Cache();
            brandSentiment.CreateOrReplaceTempView("BrandSentiment");

            var items = Mapper.MapRows(
                brandSentiment.Collect(),
                r => new BrandSentiment
                {
                    Brand = r.GetAs<string>(0),
                    SentimentRank = r.GetAs<double>(1),
                    ReviewCount = r.GetAs<int>(2)
                },
                o => o.Brand);

            context.BrandSentiment.RemoveRange(context.BrandSentiment);
            context.BrandSentiment.AddRange(items);
            context.SaveChanges();

            Console.WriteLine("Analyzing top 20 most popular brands.");

            var mostPopularBrands = spark.Sql(
                "SELECT * " +
                "FROM BrandSentiment " +
                "ORDER BY review_count DESC " +
                "LIMIT 20");

            mostPopularBrands.Show();

            Console.WriteLine("Analyzing top 20 brands with best consumer sentiment.");

            var bestBrandSentiment = spark.Sql(
                "SELECT * " +
                "FROM BrandSentiment " +
                "ORDER BY sentiment_rank DESC, review_count DESC " +
                "LIMIT 20");

            bestBrandSentiment.Show();

            Console.WriteLine("Analyzing top 20 lest popular brands.");

            var leastPopularBrands = spark.Sql(
                "SELECT * " +
                "FROM BrandSentiment " +
                "ORDER BY review_count ASC " +
                "LIMIT 20");

            leastPopularBrands.Show();

            Console.WriteLine("Analyzing top 20 brands with worst consumer sentiment.");

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

            var brands = context.BrandSentiment
                .OrderByDescending(b => b.SentimentRank)
                .ThenBy(b => b.ReviewCount)
                .Take(10)
                .Select(b => $"'{b.Brand}'")
                .ToArray();

            var brandList = string.Join(',', brands);

            var brandSentimentVsTime = spark.Sql(
                "SELECT EM.brand, FROM_UNIXTIME(ERS.unix_time, 'YYYY') as year, SUM(ERS.sentiment) / COUNT(1) * 100 as sentiment_rank " +
                "FROM ElectronicsMetadata EM " +
                "JOIN ElectronicsReviewSentiment ERS ON ERS.asin = EM.asin " +
               $"WHERE EM.brand IN ({brandList}) " +
                "GROUP BY EM.brand, FROM_UNIXTIME(ERS.unix_time, 'YYYY') " +
                "ORDER BY FROM_UNIXTIME(ERS.unix_time, 'YYYY')");

            brandSentimentVsTime.Cache();
            brandSentimentVsTime.CreateOrReplaceTempView("BrandSentimentVsTime");
            brandSentimentVsTime.Show();

            var items = Mapper.MapRows(
                brandSentimentVsTime.Collect(),
                r => new BrandSentimentVsTime
                {
                    Brand = r.GetAs<string>(0),
                    Year = int.Parse(r.GetAs<string>(1)),
                    SentimentRank = r.GetAs<double>(2)
                },
                o => $"{o.Brand}-{o.Year}");

            context.BrandSentimentVsTime.RemoveRange(context.BrandSentimentVsTime);
            context.BrandSentimentVsTime.AddRange(items);
            context.SaveChanges();
        }

        private static void AnalyseProductSentiment(SparkSession spark, CustomerSentimentContext context)
        {
            Console.WriteLine("Analyzing product consumer sentiment");

            var productSentiment = spark.Sql(
                "SELECT EM.title, EM.brand,  SUM(ERS.sentiment) / COUNT(1) * 100 as sentiment_rank, COUNT(1) review_count " +
                "FROM ElectronicsMetadata EM " +
                "JOIN ElectronicsReviewSentiment ERS ON ERS.asin = EM.asin " +
                "GROUP BY EM.title, EM.brand " +
                "HAVING COUNT(1) >= 100");

            productSentiment.Cache();
            productSentiment.CreateOrReplaceTempView("ProductSentiment");

            Console.WriteLine("Analyzing top 20 most popular products.");

            var mostPopularProducts = spark.Sql(
                "SELECT * " +
                "FROM ProductSentiment " +
                "ORDER BY review_count DESC " +
                "LIMIT 20");

            mostPopularProducts.Show();
            var rows = mostPopularProducts.Collect();

            Console.WriteLine("Analyzing top 20 products with best consumer sentiment.");

            var bestproductSentiment = spark.Sql(
                "SELECT * " +
                "FROM ProductSentiment " +
                "ORDER BY sentiment_rank DESC, review_count DESC " +
                "LIMIT 20");

            bestproductSentiment.Show();
            rows = rows.Union(bestproductSentiment.Collect());

            Console.WriteLine("Analyzing top 20 lest popular products.");

            var lestPopularProducts = spark.Sql(
                "SELECT * " +
                "FROM ProductSentiment " +
                "ORDER BY review_count ASC " +
                "LIMIT 20");

            lestPopularProducts.Show();
            rows = rows.Union(lestPopularProducts.Collect());

            Console.WriteLine("Analyzing top 20 products with worst consumer sentiment.");

            var worstproductSentiment = spark.Sql(
                "SELECT * " +
                "FROM ProductSentiment " +
                "ORDER BY sentiment_rank ASC, review_count ASC " +
                "LIMIT 20");

            worstproductSentiment.Show();
            rows = rows.Union(worstproductSentiment.Collect());

            var items = Mapper.MapRows(
                rows,
                r => new ProductSentiment
                {
                    Name = r.GetAs<string>(0),
                    Brand = r.GetAs<string>(1),
                    SentimentRank = r.GetAs<double>(2),
                    ReviewCount = r.GetAs<int>(3)

                },
                o => $"{o.Name}-{o.Brand}");

            context.ProductSentiment.RemoveRange(context.ProductSentiment);
            context.ProductSentiment.AddRange(items);
            context.SaveChanges();
        }

        private static void AnalyseCategoryDemand(SparkSession spark, CustomerSentimentContext context)
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

            var items = Mapper.MapRows(
                categoriesDemand.Collect(),
                r => new CategoryDemand
                {
                    Category = r.GetAs<string>(0),
                    Month = int.Parse(r.GetAs<string>(1)),
                    Demand = r.GetAs<int>(2)
                },
                o => $"{o.Category}-{o.Month}");

            context.CategoryDemand.RemoveRange(context.CategoryDemand);
            context.CategoryDemand.AddRange(items);
            context.SaveChangesAsync();

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

        private static void AnalyseBrandDemand(SparkSession spark, CustomerSentimentContext context)
        {
            Console.WriteLine("Analysing popular brand demand");

            var brands = context.BrandSentiment
                .OrderByDescending(b => b.ReviewCount)
                .ThenBy(b => b.SentimentRank)
                .Take(10)
                .Select(b => $"'{b.Brand}'")
                .ToArray();

            var brandList = string.Join(',', brands);

            var brandsDemand = spark.Sql(
                "SELECT EM.brand, FROM_UNIXTIME(ER.unix_time, 'MM') as month, COUNT(1) as demand " +
                "FROM ElectronicsReviews ER " +
                "JOIN ElectronicsMetadata EM ON EM.asin = ER.asin " +
               $"WHERE EM.brand IN ({brandList}) " +
                "GROUP BY EM.brand, FROM_UNIXTIME(ER.unix_time, 'MM') " +
                "ORDER BY EM.brand, FROM_UNIXTIME(ER.unix_time, 'MM')");

            brandsDemand.Cache();
            brandsDemand.CreateOrReplaceTempView("BrandsDemand");

            var items = Mapper.MapRows(
                brandsDemand.Collect(),
                r => new BrandDemand
                {
                    Brand = r.GetAs<string>(0),
                    Month = int.Parse(r.GetAs<string>(1)),
                    Demand = r.GetAs<int>(2)
                },
                o => $"{o.Brand}-{o.Month}");

            context.BrandDemand.RemoveRange(context.BrandDemand);
            context.BrandDemand.AddRange(items);
            context.SaveChanges();

            foreach (var brand in brands)
            {
                Console.WriteLine($"Analysing consumer demand for {brand}");

                var brandDemand = spark.Sql(
                    "SELECT * " +
                    "FROM BrandsDemand " +
                   $"WHERE brand = {brand}");

                brandDemand.Show();
            }
        }
    }
}
