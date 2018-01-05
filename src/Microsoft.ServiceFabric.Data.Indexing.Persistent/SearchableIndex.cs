using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;

namespace Microsoft.ServiceFabric.Data.Indexing.Persistent
{
	/// <summary>
	/// Defintion for an index that supports full-text search within a string property.
	/// This will create an <see cref="IReliableDictionary2{string, TKey[]}"/> to store the index.
	/// </summary>
	public sealed class SearchableIndex<TKey, TValue> : IIndexDefinition<TKey, TValue>
		where TKey : IComparable<TKey>, IEquatable<TKey>
	{
		public string Name { get; }
		public Func<TKey, TValue, string> Property { get; }

		private IReliableDictionary2<string, TKey[]> _index;

		/// <summary>
		/// Creates a new full-text search index.  The property value must be deterministic based on the input key and value.
		/// The value is generally a property of the key or value, but it can also be a composite or generated property.
		/// </summary>
		public SearchableIndex(string name, Func<TKey, TValue, string> property)
		{
			Name = name ?? throw new ArgumentNullException(nameof(name));
			Property = property ?? throw new ArgumentNullException(nameof(property));
		}

		/// <summary>
		/// Retrieves all keys that satisfy the given full-text search, or an empty array if there are no matches.
		/// </summary>
		public async Task<IEnumerable<TKey>> SearchAsync(ITransaction tx, string search, int count, TimeSpan timeout, CancellationToken token)
		{
			var keys = new HashSet<TKey>();

			var words = GetDistinctWords(search);
			foreach (var word in words)
			{
				var result = await _index.TryGetValueAsync(tx, word, timeout, token).ConfigureAwait(false);
				if (result.HasValue)
				{
					keys.AddRange(result.Value);
				}

				if (keys.Count >= count)
					break;
			}

			return keys.Take(count);
		}

		/// <summary>
		/// Try to load the existing reliable collection for this index and cache it.
		/// This is called internally and should not be directly called.
		/// </summary>
		async Task<bool> IIndexDefinition<TKey, TValue>.TryGetIndexAsync(IReliableStateManager stateManager, Uri baseName)
		{
			var indexName = GetIndexName(baseName);
			var result = await stateManager.TryGetAsync<IReliableDictionary2<string, TKey[]>>(indexName).ConfigureAwait(false);
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
			_index = await stateManager.GetOrAddAsync<IReliableDictionary2<string, TKey[]>>(tx, indexName, timeout).ConfigureAwait(false);
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

		/// <summary>
		/// Notify the index that a key and value was added to the primary reliable collection.
		/// This is called by <see cref="IReliableIndexedDictionary{TKey, TValue}"/> internally and should not be directly called.
		/// </summary>
		Task IIndexDefinition<TKey, TValue>.AddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken token)
		{
			var text = Property.Invoke(key, value);
			var words = GetDistinctWords(text);
			return AddAsync(tx, key, words, timeout, token);
		}

		/// <summary>
		/// Notify the index that a key and value was updated in the primary reliable collection.
		/// This is called by <see cref="IReliableIndexedDictionary{TKey, TValue}"/> internally and should not be directly called.
		/// </summary>
		async Task IIndexDefinition<TKey, TValue>.UpdateAsync(ITransaction tx, TKey key, TValue oldValue, TValue newValue, TimeSpan timeout, CancellationToken token)
		{
			var oldText = Property.Invoke(key, oldValue);
			var newText = Property.Invoke(key, newValue);

			var oldWords = GetDistinctWords(oldText);
			var newWords = GetDistinctWords(newText);

			// Ignore overlapping words that existing before and after updates.
			var wordsToRemove = oldWords.Except(newWords);
			var wordsToAdd = newWords.Except(oldWords);

			// This loses the locking order - need to adjust this.
			await RemoveAsync(tx, key, wordsToRemove, timeout, token).ConfigureAwait(false);
			await AddAsync(tx, key, wordsToAdd, timeout, token).ConfigureAwait(false);
		}

		/// <summary>
		/// Notify the index that a key and value was removed in the primary reliable collection.
		/// This is called by <see cref="IReliableIndexedDictionary{TKey, TValue}"/> internally and should not be directly called.
		/// </summary>
		Task IIndexDefinition<TKey, TValue>.RemoveAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken token)
		{
			var text = Property.Invoke(key, value);
			var words = GetDistinctWords(text);
			return RemoveAsync(tx, key, words, timeout, token);
		}

		/// <summary>
		/// Notify the index that the primary reliable collection was cleared.
		/// This is called by <see cref="IReliableIndexedDictionary{TKey, TValue}"/> internally and should not be directly called.
		/// </summary>
		Task IIndexDefinition<TKey, TValue>.ClearAsync(TimeSpan timeout, CancellationToken token)
		{
			return _index.ClearAsync(timeout, token);
		}

		private async Task AddAsync(ITransaction tx, TKey key, IEnumerable<string> words, TimeSpan timeout, CancellationToken token)
		{
			foreach (var word in words)
			{
				await _index.AddOrUpdateAsync(tx, word, f => new[] { key }, (f, keys) => keys.CopyAndAdd(key), timeout, token).ConfigureAwait(false);
			}
		}

		private async Task RemoveAsync(ITransaction tx, TKey key, IEnumerable<string> words, TimeSpan timeout, CancellationToken token)
		{
			foreach (var word in words)
			{
				// This key should exist in the index for each word.
				var result = await _index.TryGetValueAsync(tx, word, LockMode.Update, timeout, token).ConfigureAwait(false);
				if (!result.HasValue)
					throw new KeyNotFoundException();

				// Remove this key from the index.
				var updatedIndex = result.Value.CopyAndRemove(key);
				if (updatedIndex.Length > 0)
				{
					// Update the index.
					await _index.SetAsync(tx, word, updatedIndex, timeout, token).ConfigureAwait(false);
				}
				else
				{
					// Remove the index completely if this was the last key with this filter value.
					await _index.TryRemoveAsync(tx, word, timeout, token).ConfigureAwait(false);
				}
			}
		}

		private static List<string> GetDistinctWords(string text)
		{
			if (string.IsNullOrEmpty(text))
				return new List<string>();

			var words = new HashSet<string>();

			// Split on whitespace.  This doesn't work correctly for markdown (e.g. HTML/XML)
			foreach (var word in text.Split((char[])null, StringSplitOptions.RemoveEmptyEntries))
			{
				int start = 0;
				int end = word.Length;

				// Trim non-letters/digits from the start and end.
				while (start < end && !char.IsLetterOrDigit(word[start]))
					start++;
				while (end > start && !char.IsLetterOrDigit(word[end - 1]))
					end--;

				// Only add unique words that have at least one character left.
				if (start < end)
				{
					// Normalize on lower-case letters.
					words.Add(word.Substring(start, end - start).ToLower());
				}
			}

			// Sort the words, so we always take reliable collection locks in the same order.
			var result = new List<string>(words);
			result.Sort();
			return result;
		}

		private Uri GetIndexName(Uri baseName)
		{
			return new Uri(baseName, $"search/{Name}");
		}
	}
}
