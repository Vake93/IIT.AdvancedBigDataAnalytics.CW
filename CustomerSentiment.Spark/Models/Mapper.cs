using Microsoft.Spark.Sql;
using System;
using System.Collections.Generic;

namespace CustomerSentiment.Spark.Models
{
    public static class Mapper
    {
        public static IEnumerable<TValue> MapRows<TKey, TValue>(
            IEnumerable<Row> rows,
            Func<Row, TValue> mapping,
            Func<TValue, TKey> keyFinder)
        {
            var dictionary = new Dictionary<TKey, TValue>();

            foreach (var row in rows)
            {
                var mappedObject = mapping(row);
                var key = keyFinder(mappedObject);
                dictionary[key] = mappedObject;
            }

            return dictionary.Values;
        }
    }
}
