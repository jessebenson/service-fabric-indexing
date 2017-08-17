using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Indexing.Test.Mocks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.ServiceFabric.Data.Indexing.Test
{
	[TestClass]
	public class IndexExtensionsTests
	{
		[TestMethod]
		public async Task TryGetIndexed_NoIndexes()
		{
			var stateManager = new MockReliableStateManager();
			var result = await stateManager.TryGetIndexedAsync<int, string>("test");

			Assert.IsFalse(result.HasValue);
			Assert.IsNull(result.Value);
			Assert.AreEqual(0, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task TryGetIndexed_OneIndex()
		{
			var stateManager = new MockReliableStateManager();
			var result = await stateManager.TryGetIndexedAsync("test",
				new FilterableIndex<int, string, string>("index", (k, v) => v));

			Assert.IsFalse(result.HasValue);
			Assert.IsNull(result.Value);
			Assert.AreEqual(0, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task GetOrAddIndexed_NoIndexes()
		{
			var stateManager = new MockReliableStateManager();
			var dictionary = await stateManager.GetOrAddIndexedAsync<int, string>("test");

			Assert.IsNotNull(dictionary);
			Assert.AreEqual(1, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task GetOrAddIndexed_OneIndex()
		{
			var stateManager = new MockReliableStateManager();
			var dictionary = await stateManager.GetOrAddIndexedAsync("test",
				new FilterableIndex<int, string, string>("index", (k, v) => v));

			Assert.IsNotNull(dictionary);
			Assert.AreEqual(2, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task RemoveIndexed_NoIndexes()
		{
			var stateManager = new MockReliableStateManager();
			await stateManager.GetOrAddIndexedAsync<int, string>("test");
			await stateManager.RemoveIndexedAsync<int, string>("test");

			Assert.AreEqual(0, await GetReliableStateCountAsync(stateManager));
		}

		[TestMethod]
		public async Task RemoveIndexed_OneIndex()
		{
			var stateManager = new MockReliableStateManager();
			var result = await stateManager.TryGetIndexedAsync("test",
				new FilterableIndex<int, string, string>("index", (k, v) => v));
			await stateManager.RemoveIndexedAsync("test",
				new FilterableIndex<int, string, string>("index", (k, v) => v));

			Assert.AreEqual(0, await GetReliableStateCountAsync(stateManager));
		}

		private static async Task<int> GetReliableStateCountAsync(IReliableStateManager stateManager)
		{
			int count = 0;

			var enumerator = stateManager.GetAsyncEnumerator();
			while (await enumerator.MoveNextAsync(CancellationToken.None))
				count++;

			return count;
		}
	}
}
