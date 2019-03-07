using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceFabric.Extensions.Data.Indexing.Persistent
{

    public enum RangeFilterType
    {
        Inclusive,
        Exclusive
    };

    /// <summary>
    /// Defintion for a reverse index that supports filtering for exact matches on a property.
    /// This will create an <see cref="IReliableDictionary2{TFilter, TKey[]}"/> to store the index.
    /// </summary>
    public sealed class FilterableIndex<TKey, TValue, TFilter> : IIndexDefinition<TKey, TValue>
        where TKey : IComparable<TKey>, IEquatable<TKey>
        where TFilter : IComparable<TFilter>, IEquatable<TFilter>
    {
        public string Name { get; }
        public Func<TKey, TValue, TFilter> Filter { get; }
        public bool UseNaString { get; }

        public const string NAString = "**NA**";

        private IReliableDictionary2<TFilter, TKey[]> _index;

        /// <summary>
        /// Creates a new filterable index.  The filter value must be deterministic based on the input key and value.
        /// The filter value is generally a property of the key or value, but it can also be a composite or generated property.
        /// <paramref name="useNaString">whether use NA replace null string</paramref>
        /// </summary>
        public FilterableIndex(string name, Func<TKey, TValue, TFilter> filter, bool useNaString = false)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Filter = filter ?? throw new ArgumentNullException(nameof(filter));
            UseNaString = useNaString;
        }

        // Use this constructor to garauntee query-ability
        public static FilterableIndex<TKey, TValue, TFilter> CreateQueryableInstance(string valuePropertyName)
        {
            string name = valuePropertyName ?? throw new ArgumentNullException(nameof(valuePropertyName));
            if (typeof(TValue).GetProperty(valuePropertyName) == null)
            {
                throw new ArgumentException($"Dictionary value type: {typeof(TValue)} does not have property '{valuePropertyName}'. Note that nested types (e.g. Value/Name/FirstName) are not currently supported.");
            }

            // Construct the lambda we expect the index we are looking for to have
            // (k, v) => v.PropertyName
            ParameterExpression keyParameterExpression = Expression.Parameter(typeof(TKey), "k");
            ParameterExpression valueParameterExpression = Expression.Parameter(typeof(TValue), "v");
            MemberExpression propertyMemberExpression = Expression.Property(valueParameterExpression, typeof(TValue).GetProperty(valuePropertyName));
            LambdaExpression filterLambda = Expression.Lambda(propertyMemberExpression, new ParameterExpression[] { keyParameterExpression, valueParameterExpression });

            return new FilterableIndex<TKey, TValue, TFilter>(name, (Func<TKey, TValue, TFilter>)filterLambda.Compile());
        }

        private void SetFilterValueIfNull(ref TFilter filter)
        {
            if (filter == null && UseNaString)
            {
                if (typeof(TFilter) == typeof(string))
                    filter = (TFilter)Convert.ChangeType(NAString, typeof(TFilter));
            }
        }

        /// <summary>
        /// Retrieves all keys that match the given filter value, or an empty array if there are no matches.
        /// </summary>
        public async Task<IEnumerable<TKey>> FilterAsync(ITransaction tx, TFilter filter, TimeSpan timeout, CancellationToken token)
        {
            SetFilterValueIfNull(ref filter);
            var result = await _index.TryGetValueAsync(tx, filter, timeout, token).ConfigureAwait(false);
            TKey[] results = result.HasValue ? result.Value : new TKey[0];
            Array.Sort(results);
            return results;
        }

        /// <summary>
        /// Retrieves all keys that fall in the given filter range, or an empty array if there are no matches.
        /// </summary>
        public Task<IEnumerable<TKey>> RangeFilterAsync(ITransaction tx, TFilter start, RangeFilterType startType, TFilter end, RangeFilterType endType, CancellationToken token)
        {
            if (startType == RangeFilterType.Inclusive && endType == RangeFilterType.Inclusive)
            {
                return RangeFilterHelper(tx, f => start.CompareTo(f) <= 0 && f.CompareTo(end) <= 0, token);
            }
            else if (startType == RangeFilterType.Exclusive && endType == RangeFilterType.Inclusive)
            {
                return RangeFilterHelper(tx, f => start.CompareTo(f) < 0 && f.CompareTo(end) <= 0, token);
            }
            else if (startType == RangeFilterType.Inclusive && endType == RangeFilterType.Exclusive)
            {
                return RangeFilterHelper(tx, f => start.CompareTo(f) <= 0 && f.CompareTo(end) < 0, token);
            }
            else if (startType == RangeFilterType.Exclusive && endType == RangeFilterType.Exclusive)
            {
                return RangeFilterHelper(tx, f => start.CompareTo(f) < 0 && f.CompareTo(end) < 0, token);
            }
            else
            {
                throw new NotSupportedException("Impossible state, must be Inclusive / Exclusive matrix");
            }
        }

        public Task<IEnumerable<TKey>> RangeToFilterAsync(ITransaction tx, TFilter end, RangeFilterType endType, CancellationToken token)
        {
            if (endType == RangeFilterType.Inclusive)
            {
                return RangeFilterHelper(tx, f => f.CompareTo(end) <= 0, token);
            }
            else if (endType == RangeFilterType.Exclusive)
            {
                return RangeFilterHelper(tx, f => f.CompareTo(end) < 0, token);
            }
            else
            {
                throw new NotSupportedException("Impossible state, must be either Inclusive or Exclusive");
            }
        }

        public Task<IEnumerable<TKey>> RangeFromFilterAsync(ITransaction tx, TFilter start, RangeFilterType startType, CancellationToken token)
        {
            if (startType == RangeFilterType.Inclusive)
            {
                return RangeFilterHelper(tx, f => start.CompareTo(f) <= 0, token);
            }
            else if (startType == RangeFilterType.Exclusive)
            {
                return RangeFilterHelper(tx, f => start.CompareTo(f) < 0, token);
            }
            else
            {
                throw new NotSupportedException("Impossible state, must be either Inclusive or Exclusive");
            }
        }

        private async Task<IEnumerable<TKey>> RangeFilterHelper(ITransaction tx, Func<TFilter, bool> filter, CancellationToken token)
        {
            // Since filters uses exact matches, each key should appear exactly once in the index.
            var keys = new List<TKey>();

            // Include all values that fall within the range [start, end] inclusively.
            var enumerable = await _index.CreateEnumerableAsync(tx, filter, EnumerationMode.Ordered).ConfigureAwait(false);

            // Enumerate the index.
            var enumerator = enumerable.GetAsyncEnumerator();
            while (await enumerator.MoveNextAsync(token).ConfigureAwait(false))
            {
                keys.AddRange(enumerator.Current.Value);
            }

            return keys;
        }

        /// <summary>
        /// Create an async enumerable over the set of distinct filter values in this index.
        /// </summary>
        public Task<IAsyncEnumerable<TFilter>> CreateEnumerableAsync(ITransaction tx, EnumerationMode enumerationMode, TimeSpan timeout, CancellationToken token)
        {
            return _index.CreateKeyEnumerableAsync(tx, enumerationMode, timeout, token);
        }

        /// <summary>
        /// Try to load the existing reliable collection for this index and cache it.
        /// This is called internally and should not be directly called.
        /// </summary>
        async Task<bool> IIndexDefinition<TKey, TValue>.TryGetIndexAsync(IReliableStateManager stateManager, Uri baseName)
        {
            var indexName = GetIndexName(baseName);
            var result = await stateManager.TryGetAsync<IReliableDictionary2<TFilter, TKey[]>>(indexName).ConfigureAwait(false);
            if (!result.HasValue)
                return false;

            _index = result.Value;
            return true;
        }

        /// <summary>
        /// Load or create the reliable collection for this index and cache it.
        /// This is called internally and should not be directly called.
        /// </summary>
        async Task IIndexDefinition<TKey, TValue>.GetOrAddIndexAsync(ITransaction tx, IReliableStateManager stateManager, Uri baseName, TimeSpan timeout)
        {
            var indexName = GetIndexName(baseName);
            _index = await stateManager.GetOrAddAsync<IReliableDictionary2<TFilter, TKey[]>>(tx, indexName, timeout).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete the existing reliable collection for this index.
        /// This is called internally and should not be directly called.
        /// </summary>
        async Task IIndexDefinition<TKey, TValue>.RemoveIndexAsync(ITransaction tx, IReliableStateManager stateManager, Uri baseName, TimeSpan timeout)
        {
            var indexName = GetIndexName(baseName);
            await stateManager.RemoveAsync(tx, indexName, timeout).ConfigureAwait(false);
            _index = null;
        }

        private TFilter GetFilterValue(TKey key, TValue value)
        {
            var filter = Filter.Invoke(key, value);
            SetFilterValueIfNull(ref filter);
            return filter;
        }

     
        /// <summary>
        /// Notify the index that a key and value was added to the primary reliable collection.
        /// This is called by <see cref="IReliableIndexedDictionary{TKey, TValue}"/> internally and should not be directly called.
        /// </summary>
        Task IIndexDefinition<TKey, TValue>.AddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken token)
        {
            var filter = GetFilterValue(key, value);
            return AddAsync(tx, key, filter, timeout, token);
        }

        /// <summary>
        /// Notify the index that a key and value was updated in the primary reliable collection.
        /// This is called by <see cref="IReliableIndexedDictionary{TKey, TValue}"/> internally and should not be directly called.
        /// </summary>
        async Task IIndexDefinition<TKey, TValue>.UpdateAsync(ITransaction tx, TKey key, TValue oldValue, TValue newValue, TimeSpan timeout, CancellationToken token)
        {

            var oldFilter = GetFilterValue(key, oldValue);
            var newFilter = GetFilterValue(key, newValue);

            // If the filter value hasn't changed, no updates to the index are required.
            if (EqualityComparer<TFilter>.Default.Equals(oldFilter, newFilter))
                return;

            // Remove the key from the old filter index and add it to the new filter index.
            await RemoveAsync(tx, key, oldFilter, timeout, token).ConfigureAwait(false);
            await AddAsync(tx, key, newFilter, timeout, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Notify the index that a key and value was removed in the primary reliable collection.
        /// This is called by <see cref="IReliableIndexedDictionary{TKey, TValue}"/> internally and should not be directly called.
        /// </summary>
        Task IIndexDefinition<TKey, TValue>.RemoveAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken token)
        {
            var filter = GetFilterValue(key, value);
            return RemoveAsync(tx, key, filter, timeout, token);
        }

        /// <summary>
        /// Notify the index that the primary reliable collection was cleared.
        /// This is called by <see cref="IReliableIndexedDictionary{TKey, TValue}"/> internally and should not be directly called.
        /// </summary>
        Task IIndexDefinition<TKey, TValue>.ClearAsync(TimeSpan timeout, CancellationToken token)
        {
            return _index.ClearAsync(timeout, token);
        }

        private Task AddAsync(ITransaction tx, TKey key, TFilter filter, TimeSpan timeout, CancellationToken token)
        {
            return _index.AddOrUpdateAsync(tx, filter, f => new[] { key }, (f, keys) => keys.CopyAndAdd(key), timeout, token);
        }

        private async Task RemoveAsync(ITransaction tx, TKey key, TFilter filter, TimeSpan timeout, CancellationToken token)
        {
            // This key should exist in the index for the filter.
            var result = await _index.TryGetValueAsync(tx, filter, LockMode.Update, timeout, token).ConfigureAwait(false);
            if (!result.HasValue)
                throw new KeyNotFoundException();

            // Remove this key from the index.
            var updatedIndex = result.Value.CopyAndRemove(key);
            if (updatedIndex.Length > 0)
            {
                // Update the index.
                await _index.SetAsync(tx, filter, updatedIndex, timeout, token).ConfigureAwait(false);
            }
            else
            {
                // Remove the index completely if this was the last key with this filter value.
                await _index.TryRemoveAsync(tx, filter, timeout, token).ConfigureAwait(false);
            }
        }

        private Uri GetIndexName(Uri baseName)
        {
            return new Uri(baseName, $"filter/{Name}");
        }
    }
}
