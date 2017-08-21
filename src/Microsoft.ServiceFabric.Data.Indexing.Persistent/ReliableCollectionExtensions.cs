using Microsoft.ServiceFabric.Data.Collections;
using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Data.Indexing.Persistent
{
	internal enum IsolationLevel : byte
	{
		Snapshot = 0,
		ReadCommittedSnapshot = 1,
		ReadCommitted = 2,
		ReadRepeatable = 4,
	}

	internal static class ReliableCollectionExtensions
	{
		public static Task<ConditionalValue<TValue>> TryGetValueAsync<TKey, TValue>(this IReliableDictionary<TKey, TValue> dictionary, ITransaction tx, TKey key, IsolationLevel isolation, TimeSpan timeout, CancellationToken token)
			where TKey : IComparable<TKey>, IEquatable<TKey>
		{
			// Get TStore from dictionary.
			var dictionaryType = dictionary.GetType();
			var dataStoreField = dictionaryType.GetField("dataStore", BindingFlags.NonPublic | BindingFlags.Instance);
			var store = dataStoreField?.GetValue(dictionary);

			// If we can't get the underlying TStore from the dictionary, fall back to a read with default isolation level.
			if (store == null)
				return dictionary.TryGetValueAsync(tx, key, timeout, token);

			// Create underlying TStore transaction.
			var storeType = store.GetType();
			var createOrFindTransactionMethod = storeType.GetMethod("CreateOrFindTransaction", new[] { tx.GetType() });
			var createOrFindResult = createOrFindTransactionMethod.Invoke(store, new[] { tx });

			// Get the TStore transaction from the ConditionalValue<>.
			var conditionalValueType = createOrFindResult.GetType();
			var valueProperty = conditionalValueType.GetProperty("Value", BindingFlags.Public | BindingFlags.Instance);
			var storeTx = valueProperty.GetValue(createOrFindResult);

			// Set the isolation level.
			var storeTxType = storeTx.GetType();
			var isolationProperty = storeTxType.GetProperty("Isolation", BindingFlags.Public | BindingFlags.Instance);
			isolationProperty.SetValue(storeTx, Enum.ToObject(isolationProperty.PropertyType, (byte)isolation));

			// Call GetAsync() on TStore.
			var getAsyncMethod = storeType.GetMethod("GetAsync", new[] { storeTxType, typeof(TKey), typeof(TimeSpan), typeof(CancellationToken) });
			return (Task<ConditionalValue<TValue>>)getAsyncMethod.Invoke(store, new[] { storeTx, key, timeout, token });
		}
	}
}
