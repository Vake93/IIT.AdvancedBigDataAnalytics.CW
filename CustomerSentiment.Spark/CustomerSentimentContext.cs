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

        public DbSet<ProductSentiment> ProductSentiment { get; set; }

        public DbSet<CategoryDemand> CategoryDemand { get; set; }

        public DbSet<BrandDemand> BrandDemand { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder
                .Entity<ItemCategorySentiment>()
                .HasKey(p => p.Category);

            modelBuilder
                .Entity<BrandSentiment>()
                .HasKey(p => p.Brand);

            modelBuilder
                .Entity<BrandSentimentVsTime>()
                .HasKey(p => new { p.Brand, p.Year });

            modelBuilder
                .Entity<ProductSentiment>()
                .HasKey(p => p.Id);

            modelBuilder
                .Entity<CategoryDemand>()
                .HasKey(p => new { p.Category, p.Month });

            modelBuilder
                .Entity<BrandDemand>()
                .HasKey(p => new { p.Brand, p.Month });
        }
    }
}
