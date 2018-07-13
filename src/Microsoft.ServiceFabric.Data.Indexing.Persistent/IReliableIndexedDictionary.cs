using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;

namespace Microsoft.ServiceFabric.Data.Indexing.Persistent
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
		/// Creates an async enumerator over the given index of the reliable collection to retrieve all distinct index values.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IAsyncEnumerable<TFilter>> CreateIndexEnumerableAsync<TFilter>(ITransaction tx, string index) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		IAsyncEnumerable<KeyValuePair<TKey, TValue>> GetAllAsync(ITransaction tx, IEnumerable<TKey> keys, TimeSpan timeout, CancellationToken token);


		/// <summary>
		/// Creates an async enumerator over the given index of the reliable collection to retrieve all distinct index values.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IAsyncEnumerable<TFilter>> CreateIndexEnumerableAsync<TFilter>(ITransaction tx, string index, EnumerationMode enumerationMode) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		/// <summary>
		/// Creates an async enumerator over the given index of the reliable collection to retrieve all distinct index values.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IAsyncEnumerable<TFilter>> CreateIndexEnumerableAsync<TFilter>(ITransaction tx, string index, EnumerationMode enumerationMode, TimeSpan timeout, CancellationToken token) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		/// <summary>
		/// Retrieve all keys and values from the reliable collection that match the given filter using the specified index name.
		/// The index is defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> FilterAsync<TFilter>(ITransaction tx, string index, TFilter filter) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		/// <summary>
		/// Retrieve all keys and values from the reliable collection that match the given filter using the specified index name.
		/// The index is defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> FilterAsync<TFilter>(ITransaction tx, string index, TFilter filter, TimeSpan timeout, CancellationToken token) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		/// <summary>
		/// Retrieve all keys and values from the reliable collection that fall within the given range (inclusively or exclusively) using the specified index name.
		/// The index is defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> RangeFilterAsync<TFilter>(ITransaction tx, string index, TFilter startFilter, RangeFilterType startType, TFilter endFilter, RangeFilterType endType) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		/// <summary>
		/// Retrieve all keys and values from the reliable collection that fall within the given range (inclusively or exclusively) using the specified index name.
		/// The index is defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> RangeFilterAsync<TFilter>(ITransaction tx, string index, TFilter startFilter, RangeFilterType startType, TFilter endFilter, RangeFilterType endType, TimeSpan timeout, CancellationToken token) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		/// <summary>
		/// Retrieve all keys and values from the reliable collection from the beginning to the end value (inclusively or exclusively) using the specified index name.
		/// The index is defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> RangeToFilterAsync<TFilter>(ITransaction tx, string index, TFilter endFilter, RangeFilterType endType, TimeSpan timeout, CancellationToken token) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		/// <summary>
		/// Retrieve all keys and values from the reliable collection from the beginning value (inclusively or exclusively) to the end using the specified index name.
		/// The index is defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> RangeFromFilterAsync<TFilter>(ITransaction tx, string index, TFilter startFilter, RangeFilterType startType, TimeSpan timeout, CancellationToken token) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		/// <summary>
		/// Performs a full-text search over the reliable collection.  Returns all keys and values from the reliable
		/// collection that match the given search using all searchable index definitions.
		/// The indexes are defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// </summary>
		Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> SearchAsync(ITransaction tx, string search);

		/// <summary>
		/// Performs a full-text search over the reliable collection.  Returns all keys and values from the reliable
		/// collection that match the given search using all searchable index definitions.
		/// The indexes are defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// </summary>
		Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> SearchAsync(ITransaction tx, string search, int count);

		/// <summary>
		/// Performs a full-text search over the reliable collection.  Returns all keys and values from the reliable
		/// collection that match the given search using all searchable index definitions.
		/// The indexes are defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// </summary>
		Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> SearchAsync(ITransaction tx, string search, int count, TimeSpan timeout, CancellationToken token);

		/// <summary>
		/// Retrieve all keys from the reliable collection that match the given filter.
		/// The index is defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IEnumerable<TKey>> FilterKeysOnlyAsync<TFilter>(ITransaction tx, string propertyName, TFilter filter, TimeSpan timeSpan, CancellationToken cancellationToken) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		/// <summary>
		/// Retrieve all keys from the reliable collection that fall within the given range from the start value (inclusively or exclusively) through the end of the dictionary.
		/// The index is defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IEnumerable<TKey>> RangeFromFilterKeysOnlyAsync<TFilter>(ITransaction tx, string propertyName, TFilter startFilter, RangeFilterType startType, TimeSpan timeSpan, CancellationToken cancellationToken) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;

		/// <summary>
		/// Retrieve all keys from the reliable collection that fall within the given range from the start of the dictionary to the end value (inclusively or exclusively).
		/// The index is defined in the IReliableStateManager.GetOrAddIndexedAsync() call that retrieves this reliable collection.
		/// The type <typeparamref name="TFilter"/> must match the type of the <see cref="FilterableIndex{TKey, TValue, TFilter}"/>.
		/// </summary>
		Task<IEnumerable<TKey>> RangeToFilterKeysOnlyAsync<TFilter>(ITransaction tx, string propertyName, TFilter endFilter, RangeFilterType endType, TimeSpan timeSpan, CancellationToken cancellationToken) where TFilter : IComparable<TFilter>, IEquatable<TFilter>;
	}
}
