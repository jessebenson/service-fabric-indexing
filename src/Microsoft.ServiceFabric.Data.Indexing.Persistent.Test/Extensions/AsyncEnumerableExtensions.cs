using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Data.Indexing.Persistent.Test
{
	public static class AsyncEnumerableExtensions
	{
		public static async Task<IEnumerable<T>> ToEnumerable<T>(this IAsyncEnumerable<T> enumerable, CancellationToken token = default(CancellationToken))
		{
			var results = new List<T>();
			using (var enumerator = enumerable.GetAsyncEnumerator())
			{
				while (await enumerator.MoveNextAsync(token).ConfigureAwait(false))
				{
					results.Add(enumerator.Current);
				}
			}

			return results;
		}
	}
}
