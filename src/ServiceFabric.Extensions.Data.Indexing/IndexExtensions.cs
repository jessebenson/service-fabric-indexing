using System;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Data;

namespace ServiceFabric.Extensions.Data.Indexing.Persistent
{
	/// <summary>
	/// Extension methods for IReliableStateManager to support automatic reverse indexing.
	/// </summary>
	public static class IndexExtensions
	{
		private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(15);

		/// <summary>
		/// Get an <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name if it exists, or creates one with its indexes and returns it if it doesn't already exist.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static Task<IReliableIndexedDictionary<TKey, TValue>> GetOrAddIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, string name, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			return stateManager.GetOrAddIndexedAsync(name, DefaultTimeout, indexes);
		}

		/// <summary>
		/// Get an <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name if it exists, or creates one with its indexes and returns it if it doesn't already exist.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static async Task<IReliableIndexedDictionary<TKey, TValue>> GetOrAddIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, string name, TimeSpan timeout, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			using (var tx = stateManager.CreateTransaction())
			{
				var result = await stateManager.GetOrAddIndexedAsync(tx, name, timeout, indexes);
				await tx.CommitAsync().ConfigureAwait(false);
				return result;
			}
		}

		/// <summary>
		/// Get an <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name if it exists, or creates one with its indexes and returns it if it doesn't already exist.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static Task<IReliableIndexedDictionary<TKey, TValue>> GetOrAddIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, ITransaction tx, string name, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			return stateManager.GetOrAddIndexedAsync(tx, name, DefaultTimeout, indexes);
		}

		/// <summary>
		/// Get an <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name if it exists, or creates one with its indexes and returns it if it doesn't already exist.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static async Task<IReliableIndexedDictionary<TKey, TValue>> GetOrAddIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, ITransaction tx, string name, TimeSpan timeout, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			var dictionary = await stateManager.GetOrAddAsync<IReliableDictionary2<TKey, TValue>>(tx, name, timeout).ConfigureAwait(false);
			return await stateManager.GetOrAddIndexedAsync(tx, timeout, dictionary, GetBaseIndexUri(name), indexes).ConfigureAwait(false);
		}

		/// <summary>
		/// Get an <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name if it exists, or creates one with its indexes and returns it if it doesn't already exist.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static Task<IReliableIndexedDictionary<TKey, TValue>> GetOrAddIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, Uri name, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			return stateManager.GetOrAddIndexedAsync(name, DefaultTimeout, indexes);
		}

		/// <summary>
		/// Get an <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name if it exists, or creates one with its indexes and returns it if it doesn't already exist.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static async Task<IReliableIndexedDictionary<TKey, TValue>> GetOrAddIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, Uri name, TimeSpan timeout, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			using (var tx = stateManager.CreateTransaction())
			{
				var result = await stateManager.GetOrAddIndexedAsync(tx, name, timeout, indexes).ConfigureAwait(false);
				await tx.CommitAsync().ConfigureAwait(false);
				return result;
			}
		}

		/// <summary>
		/// Get an <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name if it exists, or creates one with its indexes and returns it if it doesn't already exist.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static Task<IReliableIndexedDictionary<TKey, TValue>> GetOrAddIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, ITransaction tx, Uri name, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			return stateManager.GetOrAddIndexedAsync(tx, name, DefaultTimeout, indexes);
		}

		/// <summary>
		/// Get an <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name if it exists, or creates one with its indexes and returns it if it doesn't already exist.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static async Task<IReliableIndexedDictionary<TKey, TValue>> GetOrAddIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, ITransaction tx, Uri name, TimeSpan timeout, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			var dictionary = await stateManager.GetOrAddAsync<IReliableDictionary2<TKey, TValue>>(tx, name, timeout).ConfigureAwait(false);
			return await stateManager.GetOrAddIndexedAsync(tx, timeout, dictionary, GetBaseIndexUri(name), indexes).ConfigureAwait(false);
		}

		/// <summary>
		/// Remove an existing <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name, along with its indexes.
		/// The state is permanently removed from storage and all replicas when this transaction commits.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static Task RemoveIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, string name, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			return stateManager.RemoveIndexedAsync(name, DefaultTimeout, indexes);
		}

		/// <summary>
		/// Remove an existing <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name, along with its indexes.
		/// The state is permanently removed from storage and all replicas when this transaction commits.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static async Task RemoveIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, string name, TimeSpan timeout, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			using (var tx = stateManager.CreateTransaction())
			{
				await stateManager.RemoveIndexedAsync(tx, name, timeout, indexes).ConfigureAwait(false);
				await tx.CommitAsync().ConfigureAwait(false);
			}
		}

		/// <summary>
		/// Remove an existing <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name, along with its indexes.
		/// The state is permanently removed from storage and all replicas when this transaction commits.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static Task RemoveIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, ITransaction tx, string name, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			return stateManager.RemoveIndexedAsync(tx, name, DefaultTimeout, indexes);
		}

		/// <summary>
		/// Remove an existing <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name, along with its indexes.
		/// The state is permanently removed from storage and all replicas when this transaction commits.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static async Task RemoveIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, ITransaction tx, string name, TimeSpan timeout, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			var result = await stateManager.TryGetAsync<IReliableDictionary2<TKey, TValue>>(name).ConfigureAwait(false);
			if (result.HasValue)
			{
				await stateManager.RemoveIndexedAsync(tx, result.Value.Name, timeout, indexes).ConfigureAwait(false);
			}
		}

		/// <summary>
		/// Remove an existing <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name, along with its indexes.
		/// The state is permanently removed from storage and all replicas when this transaction commits.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static Task RemoveIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, Uri name, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			return stateManager.RemoveIndexedAsync(name, DefaultTimeout, indexes);
		}

		/// <summary>
		/// Remove an existing <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name, along with its indexes.
		/// The state is permanently removed from storage and all replicas when this transaction commits.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static async Task RemoveIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, Uri name, TimeSpan timeout, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			using (var tx = stateManager.CreateTransaction())
			{
				await stateManager.RemoveIndexedAsync(tx, name, timeout, indexes).ConfigureAwait(false);
				await tx.CommitAsync().ConfigureAwait(false);
			}
		}

		/// <summary>
		/// Remove an existing <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name, along with its indexes.
		/// The state is permanently removed from storage and all replicas when this transaction commits.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static Task RemoveIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, ITransaction tx, Uri name, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			return stateManager.RemoveIndexedAsync(tx, name, DefaultTimeout, indexes);
		}

		/// <summary>
		/// Remove an existing <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name, along with its indexes.
		/// The state is permanently removed from storage and all replicas when this transaction commits.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static async Task RemoveIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, ITransaction tx, Uri name, TimeSpan timeout, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			await stateManager.RemoveAsync(tx, name, timeout).ConfigureAwait(false);

			// Remove all the indexes.
			Uri baseName = GetBaseIndexUri(name);
			foreach (var index in indexes)
			{
				await index.RemoveIndexAsync(tx, stateManager, baseName, timeout).ConfigureAwait(false);
			}
		}

		/// <summary>
		/// Attempts to get an existing <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name, along with its indexes.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static async Task<ConditionalValue<IReliableIndexedDictionary<TKey, TValue>>> TryGetIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, string name, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			var result = await stateManager.TryGetAsync<IReliableDictionary2<TKey, TValue>>(name).ConfigureAwait(false);
			if (!result.HasValue)
				return new ConditionalValue<IReliableIndexedDictionary<TKey, TValue>>();

			return await stateManager.TryGetIndexedAsync(result.Value, indexes).ConfigureAwait(false);
		}

		/// <summary>
		/// Attempts to get an existing <see cref="IReliableIndexedDictionary{TKey, TValue}"/> with the given name, along with its indexes.
		/// </summary>
		/// <remarks>
		/// The index definitions indicate the indexes that should be created, or which should exist with this reliable collection.  The index definitions should be
		/// consistent when creating/reading/removing reliable collections, and should not be changed after creation.  Doing so can cause the index to become out of sync
		/// with the primary reliable collection, which will cause runtime exceptions.
		/// </remarks>
		public static async Task<ConditionalValue<IReliableIndexedDictionary<TKey, TValue>>> TryGetAsync<TKey, TValue>(this IReliableStateManager stateManager, Uri name, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			var result = await stateManager.TryGetAsync<IReliableDictionary2<TKey, TValue>>(name).ConfigureAwait(false);
			if (!result.HasValue)
				return new ConditionalValue<IReliableIndexedDictionary<TKey, TValue>>();

			return await stateManager.TryGetIndexedAsync(result.Value, indexes).ConfigureAwait(false);
		}

		private static async Task<IReliableIndexedDictionary<TKey, TValue>> GetOrAddIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, ITransaction tx, TimeSpan timeout, IReliableDictionary2<TKey, TValue> dictionary, Uri baseName, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			// Get or create each index.
			foreach (var index in indexes)
			{
				await index.GetOrAddIndexAsync(tx, stateManager, baseName, timeout).ConfigureAwait(false);
			}

			return new ReliableIndexedDictionary<TKey, TValue>(dictionary, indexes);
		}

		private static async Task<ConditionalValue<IReliableIndexedDictionary<TKey, TValue>>> TryGetIndexedAsync<TKey, TValue>(this IReliableStateManager stateManager, IReliableDictionary2<TKey, TValue> dictionary, params IIndexDefinition<TKey, TValue>[] indexes)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			// Get or create each index.
			Uri baseName = GetBaseIndexUri(dictionary.Name);
			foreach (var index in indexes)
			{
				if (!await index.TryGetIndexAsync(stateManager, baseName).ConfigureAwait(false))
					return new ConditionalValue<IReliableIndexedDictionary<TKey, TValue>>();
			}

			var result = new ReliableIndexedDictionary<TKey, TValue>(dictionary, indexes);
			return new ConditionalValue<IReliableIndexedDictionary<TKey, TValue>>(true, result);
		}

		private static Uri GetBaseIndexUri(Uri name)
		{
			return GetBaseIndexUri(name.AbsolutePath);
		}

		private static Uri GetBaseIndexUri(string name)
		{
			// Ensure the base Uri ends with a '/' so it's easy to construct relative Uri's from it for the indexes.
			return new Uri($"index:/{name.TrimEnd('/')}/");
		}
	}
}
