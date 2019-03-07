using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Data
{
    /// <summary>
    /// Extensions for IAsyncEnumerable
    /// </summary>
    public static class AsyncEnumerableExtensions
    {
        /// <summary>
        /// Get list for TValue
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="self"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<List<TValue>> ToListAsync<TKey, TValue>(this IAsyncEnumerable<KeyValuePair<TKey, TValue>> self, CancellationToken cancellationToken)
        {
            if (self == null)
                throw new ArgumentNullException(nameof(self));

            var result = new List<TValue>();
            var enumerator = self.GetAsyncEnumerator();
            while (await enumerator.MoveNextAsync(cancellationToken))
            {
                result.Add(enumerator.Current.Value);
            }
            return result;
        }

        /// <summary>
        /// Search data for IAsyncEnumerable with match
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="self"></param>
        /// <param name="match"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<List<TValue>> FindAllAsync<TKey, TValue>(this IAsyncEnumerable<KeyValuePair<TKey, TValue>> self, Predicate<TValue> match, CancellationToken cancellationToken)
        {
            if (self == null)
                throw new ArgumentNullException(nameof(self));

            var result = new List<TValue>();
            var enumerator = self.GetAsyncEnumerator();
            while (await enumerator.MoveNextAsync(cancellationToken))
            {
                if (match(enumerator.Current.Value))
                    result.Add(enumerator.Current.Value);
            }
            return result;
        }
    }
}
