using CustomerSentiment.Spark.Models;
using Microsoft.EntityFrameworkCore;

namespace CustomerSentiment.Spark
{
    public class CustomerSentimentContext : DbContext
    {
        public CustomerSentimentContext(DbContextOptions<CustomerSentimentContext> options)
            : base(options)
        {
        }

        public DbSet<ItemCategorySentiment> ItemCategorySentiment { get; set; }

        public DbSet<BrandSentiment> BrandSentiment { get; set; }

        public DbSet<BrandSentimentVsTime> BrandSentimentVsTime { get; set; }
    }
}
