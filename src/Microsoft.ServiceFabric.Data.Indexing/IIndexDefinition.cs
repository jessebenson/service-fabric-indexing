using System;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Data.Indexing
{
	public interface IIndexDefinition<TKey, TValue>
		where TKey : IComparable<TKey>, IEquatable<TKey>
	{
		string Name { get; }

		Task<bool> TryGetIndexAsync(IReliableStateManager stateManager, Uri baseName);
		Task GetOrAddIndexAsync(ITransaction tx, IReliableStateManager stateManager, Uri baseName, TimeSpan timeout);
		Task RemoveIndexAsync(ITransaction tx, IReliableStateManager stateManager, Uri baseName, TimeSpan timeout);

		Task AddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken token);
		Task UpdateAsync(ITransaction tx, TKey key, TValue oldValue, TValue newValue, TimeSpan timeout, CancellationToken token);
		Task RemoveAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken token);
		Task ClearAsync(TimeSpan timeout, CancellationToken token);
	}
}
