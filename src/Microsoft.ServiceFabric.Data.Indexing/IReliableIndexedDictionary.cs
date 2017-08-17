using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;

namespace Microsoft.ServiceFabric.Data.Indexing
{
	/// <summary>
	/// Represents a reliable collection of key/value pairs that are persisted and replicated, with support for reverse indexing and full-text search.
	/// 
	/// To use, call the 'Indexed' variants of the IReliableStateManager methods:
	/// - GetOrAddIndexedAsync() instead of GetOrAddAsync()
	/// - TryGetIndexedAsync() instead of TryGetAsync()
	/// - RemoveIndexedAsync() instead of RemoveAsync()
	/// </summary>
	public interface IReliableIndexedDictionary<TKey, TValue> : IReliableDictionary2<TKey, TValue>
		where TKey : IComparable<TKey>, IEquatable<TKey>
	{
		/// <summary>
		/// Retrieve all keys and values from the reliable collection that match the given filter using the specified index name.
		/// The index is defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IEnumerable<KeyValuePair<TKey, TValue>>> FilterAsync<TFilter>(ITransaction tx, string index, TFilter filter) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		/// <summary>
		/// Retrieve all keys and values from the reliable collection that match the given filter using the specified index name.
		/// The index is defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		Task<IEnumerable<KeyValuePair<TKey, TValue>>> FilterAsync<TFilter>(ITransaction tx, string index, TFilter filter, TimeSpan timeout, CancellationToken token) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		/// <summary>
		/// Performs a full-text search over the reliable collection.  Returns all keys and values from the reliable
		/// collection that match the given search using all searchable index definitions.
		/// The indexes are defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		Task<IEnumerable<KeyValuePair<TKey, TValue>>> SearchAsync(ITransaction tx, string search);

		/// <summary>
		/// Performs a full-text search over the reliable collection.  Returns all keys and values from the reliable
		/// collection that match the given search using all searchable index definitions.
		/// The indexes are defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		Task<IEnumerable<KeyValuePair<TKey, TValue>>> SearchAsync(ITransaction tx, string search, TimeSpan timeout, CancellationToken token);
	}
}
