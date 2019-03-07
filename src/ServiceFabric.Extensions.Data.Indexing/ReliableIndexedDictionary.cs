using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Data.Notifications;
using Microsoft.ServiceFabric.Data;

namespace ServiceFabric.Extensions.Data.Indexing.Persistent
{
	internal class ReliableIndexedDictionary<TKey, TValue> : IReliableIndexedDictionary<TKey, TValue>
		where TKey : IComparable<TKey>, IEquatable<TKey>
	{
		private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(15);

		private readonly IReliableDictionary2<TKey, TValue> _dictionary;
		private readonly IIndexDefinition<TKey, TValue>[] _indexes;
		private readonly IDictionary<string, IIndexDefinition<TKey, TValue>> _filterIndexes;
		private readonly IDictionary<string, SearchableIndex<TKey, TValue>> _searchIndexes;

		public event EventHandler<NotifyDictionaryChangedEventArgs<TKey, TValue>> DictionaryChanged
		{
			add { _dictionary.DictionaryChanged += value; }
			remove { _dictionary.DictionaryChanged -= value; }
		}

		Func<IReliableDictionary<TKey, TValue>, NotifyDictionaryRebuildEventArgs<TKey, TValue>, Task> IReliableDictionary<TKey, TValue>.RebuildNotificationAsyncCallback
		{
			set
			{
				_dictionary.RebuildNotificationAsyncCallback = value;
			}
		}

		public long Count => _dictionary.Count;

		public Uri Name => _dictionary.Name;

		public ReliableIndexedDictionary(IReliableDictionary2<TKey, TValue> dictionary, params IIndexDefinition<TKey, TValue>[] indexes)
		{
			_dictionary = dictionary ?? throw new ArgumentNullException(nameof(dictionary));
			_indexes = indexes ?? throw new ArgumentNullException(nameof(indexes));

			_filterIndexes = indexes.Where(index => index.GetType().GetGenericTypeDefinition() == typeof(FilterableIndex<,,>)).ToDictionary(index => index.Name);
			_searchIndexes = indexes.Where(index => index is SearchableIndex<TKey, TValue>).Select(index => index as SearchableIndex<TKey, TValue>).ToDictionary(index => index.Name);
		}

		public Task<long> GetCountAsync(ITransaction tx)
		{
			return _dictionary.GetCountAsync(tx);
		}

		public Task AddAsync(ITransaction tx, TKey key, TValue value)
		{
			return AddAsync(tx, key, value, DefaultTimeout, CancellationToken.None);
		}

		public async Task AddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
		{
			await _dictionary.AddAsync(tx, key, value, timeout, cancellationToken).ConfigureAwait(false);
			await OnAddAsync(tx, key, value, timeout, cancellationToken).ConfigureAwait(false);
		}

		public Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
		{
			return AddOrUpdateAsync(tx, key, addValue, updateValueFactory, DefaultTimeout, CancellationToken.None);
		}

		public Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return AddOrUpdateAsync(tx, key, k => addValue, updateValueFactory, timeout, cancellationToken);
		}

		public Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
		{
			return AddOrUpdateAsync(tx, key, addValueFactory, updateValueFactory, DefaultTimeout, CancellationToken.None);
		}

		public async Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory, TimeSpan timeout, CancellationToken cancellationToken)
		{
			bool isAdd = false;
			TValue value = default(TValue);
			TValue oldValue = default(TValue);

			var result = await _dictionary.AddOrUpdateAsync(
				tx,
				key,
				k => { isAdd = true; value = addValueFactory(k); return value; },
				(k, v) => { isAdd = false; oldValue = v; value = updateValueFactory(k, v); return value; },
				timeout,
				cancellationToken).ConfigureAwait(false);

			if (isAdd)
			{
				await OnAddAsync(tx, key, value, timeout, cancellationToken).ConfigureAwait(false);
			}
			else
			{
				await OnUpdateAsync(tx, key, oldValue, value, timeout, cancellationToken).ConfigureAwait(false);
			}

			return result;
		}

		public Task ClearAsync()
		{
			return ClearAsync(DefaultTimeout, CancellationToken.None);
		}

		public async Task ClearAsync(TimeSpan timeout, CancellationToken cancellationToken)
		{
			await _dictionary.ClearAsync(timeout, cancellationToken).ConfigureAwait(false);

			foreach (var index in _indexes)
			{
				await index.ClearAsync(timeout, cancellationToken).ConfigureAwait(false);
			}
		}

		public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key)
		{
			return _dictionary.ContainsKeyAsync(tx, key);
		}

		public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key, LockMode lockMode)
		{
			return _dictionary.ContainsKeyAsync(tx, key, lockMode);
		}

		public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return _dictionary.ContainsKeyAsync(tx, key, timeout, cancellationToken);
		}

		public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key, LockMode lockMode, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return _dictionary.ContainsKeyAsync(tx, key, lockMode, timeout, cancellationToken);
		}

		Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> IReliableDictionary<TKey, TValue>.CreateEnumerableAsync(ITransaction tx)
		{
			return _dictionary.CreateEnumerableAsync(tx);
		}

		Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> IReliableDictionary<TKey, TValue>.CreateEnumerableAsync(ITransaction tx, EnumerationMode enumerationMode)
		{
			return _dictionary.CreateEnumerableAsync(tx, enumerationMode);
		}

		Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> IReliableDictionary<TKey, TValue>.CreateEnumerableAsync(ITransaction tx, Func<TKey, bool> filter, EnumerationMode enumerationMode)
		{
			return _dictionary.CreateEnumerableAsync(tx, filter, enumerationMode);
		}

		public Task<IAsyncEnumerable<TKey>> CreateKeyEnumerableAsync(ITransaction tx)
		{
			return _dictionary.CreateKeyEnumerableAsync(tx);
		}

		public Task<IAsyncEnumerable<TKey>> CreateKeyEnumerableAsync(ITransaction tx, EnumerationMode enumerationMode)
		{
			return _dictionary.CreateKeyEnumerableAsync(tx, enumerationMode);
		}

		public Task<IAsyncEnumerable<TKey>> CreateKeyEnumerableAsync(ITransaction tx, EnumerationMode enumerationMode, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return _dictionary.CreateKeyEnumerableAsync(tx, enumerationMode, timeout, cancellationToken);
		}

		public Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, TValue value)
		{
			return GetOrAddAsync(tx, key, value, DefaultTimeout, CancellationToken.None);
		}

		public Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return GetOrAddAsync(tx, key, k => value, timeout, cancellationToken);
		}

		public Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, Func<TKey, TValue> valueFactory)
		{
			return GetOrAddAsync(tx, key, valueFactory, DefaultTimeout, CancellationToken.None);
		}

		public async Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, Func<TKey, TValue> valueFactory, TimeSpan timeout, CancellationToken cancellationToken)
		{
			bool isAdd = false;
			TValue value = default(TValue);

			var result = await _dictionary.GetOrAddAsync(tx, key, k => { isAdd = true; value = valueFactory(k); return value; }, timeout, cancellationToken).ConfigureAwait(false);

			if (isAdd)
			{
				await OnAddAsync(tx, key, value, timeout, cancellationToken).ConfigureAwait(false);
			}

			return result;
		}

		public Task SetAsync(ITransaction tx, TKey key, TValue value)
		{
			return SetAsync(tx, key, value, DefaultTimeout, CancellationToken.None);
		}

		public async Task SetAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
		{
			bool isUpdate = false;
			TValue oldValue = default(TValue);

			// Set is equivalent to AddOrUpdate.  We monitor the update callback to know whether the operation was an Add or Update.
			await _dictionary.AddOrUpdateAsync(tx, key, value, (k, v) => { isUpdate = true; oldValue = v; return value; }, timeout, cancellationToken).ConfigureAwait(false);

			if (isUpdate)
			{
				await OnUpdateAsync(tx, key, oldValue, value, timeout, cancellationToken).ConfigureAwait(false);
			}
			else
			{
				await OnAddAsync(tx, key, value, timeout, cancellationToken).ConfigureAwait(false);
			}
		}

		public Task<bool> TryAddAsync(ITransaction tx, TKey key, TValue value)
		{
			return TryAddAsync(tx, key, value, DefaultTimeout, CancellationToken.None);
		}

		public async Task<bool> TryAddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
		{
			var result = await _dictionary.TryAddAsync(tx, key, value, timeout, cancellationToken).ConfigureAwait(false);
			if (result)
			{
				await OnAddAsync(tx, key, value, timeout, cancellationToken).ConfigureAwait(false);
			}

			return result;
		}

		public Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key)
		{
			return _dictionary.TryGetValueAsync(tx, key);
		}

		public Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key, LockMode lockMode)
		{
			return _dictionary.TryGetValueAsync(tx, key, lockMode);
		}

		public Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return _dictionary.TryGetValueAsync(tx, key, timeout, cancellationToken);
		}

		public Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key, LockMode lockMode, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return _dictionary.TryGetValueAsync(tx, key, lockMode, timeout, cancellationToken);
		}

		public Task<ConditionalValue<TValue>> TryRemoveAsync(ITransaction tx, TKey key)
		{
			return TryRemoveAsync(tx, key, DefaultTimeout, CancellationToken.None);
		}

		public async Task<ConditionalValue<TValue>> TryRemoveAsync(ITransaction tx, TKey key, TimeSpan timeout, CancellationToken cancellationToken)
		{
			var result = await _dictionary.TryRemoveAsync(tx, key, timeout, cancellationToken).ConfigureAwait(false);
			if (result.HasValue)
			{
				await OnRemoveAsync(tx, key, result.Value, timeout, cancellationToken).ConfigureAwait(false);
			}

			return result;
		}

		public Task<bool> TryUpdateAsync(ITransaction tx, TKey key, TValue newValue, TValue comparisonValue)
		{
			return TryUpdateAsync(tx, key, newValue, comparisonValue, DefaultTimeout, CancellationToken.None);
		}

		public async Task<bool> TryUpdateAsync(ITransaction tx, TKey key, TValue newValue, TValue comparisonValue, TimeSpan timeout, CancellationToken cancellationToken)
		{
			// We can't trust that comparisonValue is exactly equal to the old value, so we have to read the precise old value first.
			var current = await _dictionary.TryGetValueAsync(tx, key, LockMode.Update, timeout, cancellationToken).ConfigureAwait(false);
			if (!current.HasValue)
				return false;

			var result = await _dictionary.TryUpdateAsync(tx, key, newValue, comparisonValue, timeout, cancellationToken).ConfigureAwait(false);
			if (result)
			{
				await OnUpdateAsync(tx, key, current.Value, newValue, timeout, cancellationToken).ConfigureAwait(false);
			}

			return result;
		}

		public Task<IAsyncEnumerable<TFilter>> CreateIndexEnumerableAsync<TFilter>(ITransaction tx, string index)
			where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			return CreateIndexEnumerableAsync<TFilter>(tx, index, EnumerationMode.Unordered, DefaultTimeout, CancellationToken.None);
		}

		public Task<IAsyncEnumerable<TFilter>> CreateIndexEnumerableAsync<TFilter>(ITransaction tx, string index, EnumerationMode enumerationMode)
			where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			return CreateIndexEnumerableAsync<TFilter>(tx, index, enumerationMode, DefaultTimeout, CancellationToken.None);
		}

		public Task<IAsyncEnumerable<TFilter>> CreateIndexEnumerableAsync<TFilter>(ITransaction tx, string indexName, EnumerationMode enumerationMode, TimeSpan timeout, CancellationToken token)
			where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			// Find the index.
			var index = GetFilterableIndex<TFilter>(indexName);

			// Enumerate the keys (distinct filter values) of this index.
			return index.CreateEnumerableAsync(tx, enumerationMode, timeout, token);
		}

		public Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> FilterAsync<TFilter>(ITransaction tx, string index, TFilter filter)
			where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			return FilterAsync(tx, index, filter, DefaultTimeout, CancellationToken.None);
		}

		public async Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> FilterAsync<TFilter>(ITransaction tx, string indexName, TFilter filter, TimeSpan timeout, CancellationToken token)
			where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			// Find the index.
			var index = GetFilterableIndex<TFilter>(indexName);

			// Find the keys that match this filter.
			var keys = await index.FilterAsync(tx, filter, timeout, token).ConfigureAwait(false);

			// Get the rows that match this filter.
			return GetAllAsync(tx, keys, timeout, token);
		}

		public Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> RangeFilterAsync<TFilter>(ITransaction tx, string index, TFilter startFilter, RangeFilterType startType, TFilter endFilter, RangeFilterType endType)
			where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			return RangeFilterAsync(tx, index, startFilter, startType, endFilter, endType, DefaultTimeout, CancellationToken.None);
		}

		public async Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> RangeFilterAsync<TFilter>(ITransaction tx, string indexName, TFilter startFilter, RangeFilterType startType, TFilter endFilter, RangeFilterType endType, TimeSpan timeout, CancellationToken token)
			where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			// Find the index.
			var index = GetFilterableIndex<TFilter>(indexName);

			// Find the keys that fall within this range (inclusively or exclusively).
			var keys = await index.RangeFilterAsync(tx, startFilter, startType, endFilter, endType, token).ConfigureAwait(false);

			// Get the rows that match this filter.
			return GetAllAsync(tx, keys, timeout, token);
		}

		public Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> SearchAsync(ITransaction tx, string search)
		{
			return SearchAsync(tx, search, int.MaxValue, DefaultTimeout, CancellationToken.None);
		}

		public Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> SearchAsync(ITransaction tx, string search, int count)
		{
			return SearchAsync(tx, search, count, DefaultTimeout, CancellationToken.None);
		}

		public async Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> SearchAsync(ITransaction tx, string search, int count, TimeSpan timeout, CancellationToken token)
		{
			if (_searchIndexes.Count == 0)
				throw new InvalidOperationException("You must define at least one SearchableIndex to enable full-text search.");

			// Find all distinct keys that match this search.
			var keys = new HashSet<TKey>();
			foreach (var searchIndex in _searchIndexes.Values)
			{
				var partialKeys = await searchIndex.SearchAsync(tx, search, count, timeout, token).ConfigureAwait(false);
				keys.AddRange(partialKeys);
			}

            // Get all rows that match this search.
            return GetAllAsync(tx, keys, timeout, token);
		}

		public IAsyncEnumerable<KeyValuePair<TKey, TValue>> GetAllAsync(ITransaction tx, IEnumerable<TKey> keys, TimeSpan timeout, CancellationToken token)
		{
            return new DictionaryFetchAsyncEnumerable<TKey, TValue>(tx, this, keys, timeout, token);
		}

		private FilterableIndex<TKey, TValue, TFilter> GetFilterableIndex<TFilter>(string indexName)
			where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			// Find the index.
			if (!_filterIndexes.TryGetValue(indexName, out IIndexDefinition<TKey, TValue> definition))
				throw new KeyNotFoundException($"Index '{indexName}' not found.");

			// Ensure the index is of the correct type.
			var index = definition as FilterableIndex<TKey, TValue, TFilter>;
			if (index == null)
				throw new InvalidCastException($"Index '{indexName}' is not a filterable index of this type.");

			return index;
		}

		/// <summary>
		/// On successful adds, we notify each index of the newly added key and value so it can be updated.
		/// </summary>
		/// <devnote>
		/// Since filter/search using indexes first locks the indexes, index locks should be taken before dictionary locks.
		/// Until this is implemented, we are more likely to hit timeout exceptions when conflicting operations occur.
		/// </devnote>
		private async Task OnAddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken token)
		{
			foreach (var index in _indexes)
			{
				await index.AddAsync(tx, key, value, timeout, token).ConfigureAwait(false);
			}
		}

		/// <summary>
		/// On successful updates, we notify each index of the previous and current key and value so it can be updated.
		/// </summary>
		/// <devnote>
		/// Since filter/search using indexes first locks the indexes, index locks should be taken before dictionary locks.
		/// Until this is implemented, we are more likely to hit timeout exceptions when conflicting operations occur.
		/// </devnote>
		private async Task OnUpdateAsync(ITransaction tx, TKey key, TValue oldValue, TValue newValue, TimeSpan timeout, CancellationToken token)
		{
			foreach (var index in _indexes)
			{
				await index.UpdateAsync(tx, key, oldValue, newValue, timeout, token).ConfigureAwait(false);
			}
		}

		/// <summary>
		/// On successful removes, we notify each index of the now removed key and value so it can be updated.
		/// </summary>
		/// <devnote>
		/// Since filter/search using indexes first locks the indexes, index locks should be taken before dictionary locks.
		/// Until this is implemented, we are more likely to hit timeout exceptions when conflicting operations occur.
		/// </devnote>
		private async Task OnRemoveAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken token)
		{
			foreach (var index in _indexes)
			{
				await index.RemoveAsync(tx, key, value, timeout, token).ConfigureAwait(false);
			}
		}

		public async Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> RangeToFilterAsync<TFilter>(ITransaction tx, string indexName, TFilter endFilter, RangeFilterType endType, TimeSpan timeout, CancellationToken token) where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			// Find the index.
			var index = GetFilterableIndex<TFilter>(indexName);

			// Find the keys from the start to the end value (inclusively or exclusively).
			var keys = await index.RangeToFilterAsync(tx, endFilter, endType, token).ConfigureAwait(false);

			// Get the rows that match this filter.
			return GetAllAsync(tx, keys, timeout, token);
		}

		public async Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> RangeFromFilterAsync<TFilter>(ITransaction tx, string indexName, TFilter startFilter, RangeFilterType startType, TimeSpan timeout, CancellationToken token) where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			// Find the index.
			var index = GetFilterableIndex<TFilter>(indexName);

			// Find the keys from the start value (inclusively or exclusively) to the end.
			var keys = await index.RangeFromFilterAsync(tx, startFilter, startType, token).ConfigureAwait(false);

			// Get the rows that match this filter.
			return GetAllAsync(tx, keys, timeout, token);
		}

		public Task<IEnumerable<TKey>> RangeFilterKeysOnlyAsync<TFilter>(ITransaction tx, string indexName, TFilter startFilter, RangeFilterType startType, TFilter endFilter, RangeFilterType endType, TimeSpan timeout, CancellationToken token)
			where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			// Find the index.
			var index = GetFilterableIndex<TFilter>(indexName);

			// Find the keys that fall within this range (inclusively).
			return index.RangeFilterAsync(tx, startFilter, startType, endFilter, endType, token);
		}

		public Task<IEnumerable<TKey>> RangeToFilterKeysOnlyAsync<TFilter>(ITransaction tx, string indexName, TFilter endFilter, RangeFilterType endType, TimeSpan timeout, CancellationToken token)
			where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			// Find the index.
			var index = GetFilterableIndex<TFilter>(indexName);

			// Find the keys from the start to the end value (inclusively or exclusively).
			return index.RangeToFilterAsync(tx, endFilter, endType, token);
		}

		public Task<IEnumerable<TKey>> RangeFromFilterKeysOnlyAsync<TFilter>(ITransaction tx, string indexName, TFilter startFilter, RangeFilterType startType, TimeSpan timeout, CancellationToken token)
			where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			// Find the index.
			var index = GetFilterableIndex<TFilter>(indexName);

			// Find the keys from the start value (inclusively or exclusively) to the end.
			return index.RangeFromFilterAsync(tx, startFilter, startType, token);
		}

		public Task<IEnumerable<TKey>> FilterKeysOnlyAsync<TFilter>(ITransaction tx, string indexName, TFilter filter, TimeSpan timeout, CancellationToken token)
			where TFilter : IComparable<TFilter>, IEquatable<TFilter>
		{
			// Find the index.
			var index = GetFilterableIndex<TFilter>(indexName);

			// Find the keys that match this filter.
			return index.FilterAsync(tx, filter, timeout, token);
		}
	}
}
