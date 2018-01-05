using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Data.Indexing.Persistent.Test.Models;
using Microsoft.ServiceFabric.Data.Mocks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.ServiceFabric.Data.Indexing.Persistent.Test
{
	[TestClass]
	public class FilterableIndexTests
	{
		private static readonly Random Random = new Random();

		[TestMethod]
		public async Task StringFilter_Add()
		{
			var stateManager = new MockReliableStateManager();
			var dictionary = await stateManager.GetOrAddIndexedAsync("test",
				new FilterableIndex<Guid, Person, string>("name", (k, p) => p.Name));

			// Add person using normal IReliableDictionary APIs.  This should update the index as well.
			var john = new Person { Name = "John" };
			using (var tx = stateManager.CreateTransaction())
			{
				await dictionary.AddAsync(tx, john.Id, john);
				await tx.CommitAsync();
			}

			using (var tx = stateManager.CreateTransaction())
			{
				// Search the index for this person's name.  This should return the person we added above.
				var result = await dictionary.FilterAsync(tx, "name", "John");
				Assert.AreEqual(1, result.Count());
				Assert.AreEqual(john.Id, result.First().Key);
				Assert.AreSame(john, result.First().Value);

				// Search the index for the wrong name.  This should not return any results.
				var nobody = await dictionary.FilterAsync(tx, "name", "Jane");
				Assert.AreEqual(0, nobody.Count());

				await tx.CommitAsync();
			}
		}

		[TestMethod]
		public async Task StringFilter_Remove()
		{
			var stateManager = new MockReliableStateManager();
			var dictionary = await stateManager.GetOrAddIndexedAsync("test",
				new FilterableIndex<Guid, Person, string>("name", (k, p) => p.Name));

			// Add then remove person using normal IReliableDictionary APIs.  This should update the index as well.
			var john = new Person { Name = "John" };
			using (var tx = stateManager.CreateTransaction())
			{
				await dictionary.AddAsync(tx, john.Id, john);
				var remove = await dictionary.TryRemoveAsync(tx, john.Id);
				await tx.CommitAsync();

				Assert.IsTrue(remove.HasValue);
			}

			using (var tx = stateManager.CreateTransaction())
			{
				// Search the index for this person's name.  This should not return the person above.
				var result = await dictionary.FilterAsync(tx, "name", "John");
				Assert.AreEqual(0, result.Count());

				await tx.CommitAsync();
			}
		}

		[TestMethod]
		public async Task StringFilter_Update()
		{
			var stateManager = new MockReliableStateManager();
			var dictionary = await stateManager.GetOrAddIndexedAsync("test",
				new FilterableIndex<Guid, Person, string>("name", (k, p) => p.Name));

			// Add then update person using normal IReliableDictionary APIs.  This should update the index as well.
			var john = new Person { Name = "John" };
			var jane = new Person { Name = "Jane" };

			using (var tx = stateManager.CreateTransaction())
			{
				await dictionary.AddAsync(tx, john.Id, john);
				await dictionary.SetAsync(tx, john.Id, jane);
				await tx.CommitAsync();
			}

			using (var tx = stateManager.CreateTransaction())
			{
				// Search the index for John.  This should not return anything.
				var result = await dictionary.FilterAsync(tx, "name", "John");
				Assert.AreEqual(0, result.Count());

				// Search the index for Jane.  This should return the Jane person.
				result = await dictionary.FilterAsync(tx, "name", "Jane");
				Assert.AreEqual(1, result.Count());
				CollectionAssert.Contains(result.Select(x => x.Value).ToArray(), jane);

				await tx.CommitAsync();
			}
		}

		[TestMethod]
		public async Task StringFilter_AddMultiple()
		{
			var stateManager = new MockReliableStateManager();
			var dictionary = await stateManager.GetOrAddIndexedAsync("test",
				new FilterableIndex<Guid, Person, string>("name", (k, p) => p.Name));

			// Add people using normal IReliableDictionary APIs.  This should update the index as well.
			var john1 = new Person { Name = "John", Age = 23 };
			var john2 = new Person { Name = "John", Age = 35 };
			using (var tx = stateManager.CreateTransaction())
			{
				await dictionary.AddAsync(tx, john1.Id, john1);
				await dictionary.AddAsync(tx, john2.Id, john2);
				await tx.CommitAsync();
			}

			using (var tx = stateManager.CreateTransaction())
			{
				// Search the index for this person's name.  This should return the people we added above.
				var results = await dictionary.FilterAsync(tx, "name", "John");
				Assert.AreEqual(2, results.Count());
				CollectionAssert.Contains(results.Select(x => x.Value).ToArray(), john1);
				CollectionAssert.Contains(results.Select(x => x.Value).ToArray(), john2);

				// Search the index for the wrong name.  This should not return any results.
				var nobody = await dictionary.FilterAsync(tx, "name", "Jane");
				Assert.AreEqual(0, nobody.Count());

				// Search the index for this person's name with a count limit.  This should return one of people we added above.
				var single = await dictionary.FilterAsync(tx, "name", "John", count: 1);
				Assert.AreEqual(1, single.Count());
				var singleActual = results.Select(x => x.Value).First();
				Assert.IsTrue(singleActual == john1 || singleActual == john2);

				await tx.CommitAsync();
			}
		}

		[TestMethod]
		public async Task RangeFilter()
		{
			var stateManager = new MockReliableStateManager();
			var dictionary = await stateManager.GetOrAddIndexedAsync("test",
				new FilterableIndex<Guid, Person, int>("age", (k, p) => p.Age));

			// Add people using normal IReliableDictionary APIs.  This should update the index as well.
			var john = new Person { Name = "John", Age = 32 };
			var jane = new Person { Name = "Jane", Age = 25 };
			var mary = new Person { Name = "Mary", Age = 35 };

			using (var tx = stateManager.CreateTransaction())
			{
				await dictionary.AddAsync(tx, john.Id, john);
				await dictionary.AddAsync(tx, jane.Id, jane);
				await dictionary.AddAsync(tx, mary.Id, mary);
				await tx.CommitAsync();
			}

			using (var tx = stateManager.CreateTransaction())
			{
				// Range filter - range too low
				var results = await dictionary.RangeFilterAsync(tx, "age", 0, 10);
				Assert.AreEqual(0, results.Count());

				// Range filter - range too high.
				results = await dictionary.RangeFilterAsync(tx, "age", 70, 100);
				Assert.AreEqual(0, results.Count());

				// Range filter - fully included (order is important).
				results = await dictionary.RangeFilterAsync(tx, "age", 0, 100);
				Assert.AreEqual(3, results.Count());
				CollectionAssert.AreEqual(new[] { jane, john, mary }, results.Select(x => x.Value).ToArray());

				// Range filter - partially included.
				results = await dictionary.RangeFilterAsync(tx, "age", 30, 40);
				Assert.AreEqual(2, results.Count());
				CollectionAssert.AreEqual(new[] { john, mary }, results.Select(x => x.Value).ToArray());

				// Range filter - partially included, start overlaps.
				results = await dictionary.RangeFilterAsync(tx, "age", 32, 40);
				Assert.AreEqual(2, results.Count());
				CollectionAssert.AreEqual(new[] { john, mary }, results.Select(x => x.Value).ToArray());

				// Range filter - partially included, end overlaps.
				results = await dictionary.RangeFilterAsync(tx, "age", 30, 35);
				Assert.AreEqual(2, results.Count());
				CollectionAssert.AreEqual(new[] { john, mary }, results.Select(x => x.Value).ToArray());

				// Range filter - partially included, in the middle.
				results = await dictionary.RangeFilterAsync(tx, "age", 30, 33);
				Assert.AreEqual(1, results.Count());
				CollectionAssert.AreEqual(new[] { john }, results.Select(x => x.Value).ToArray());

				// Range filter - partially included, count limit.
				results = await dictionary.RangeFilterAsync(tx, "age", 30, 35, count: 1);
				Assert.AreEqual(1, results.Count());
				var singleActual = results.Select(x => x.Value).First();
				Assert.IsTrue(singleActual == john || singleActual == mary);

				await tx.CommitAsync();
			}
		}

		[TestMethod]
		public async Task EnumerateFilters()
		{
			var stateManager = new MockReliableStateManager();
			var dictionary = await stateManager.GetOrAddIndexedAsync("test",
				new FilterableIndex<Guid, Person, int>("age", (k, p) => p.Age));

			// Add people using normal IReliableDictionary APIs.  This should update the index as well.
			var john = new Person { Name = "John", Age = 32 };
			var jane = new Person { Name = "Jane", Age = 25 };
			var mary = new Person { Name = "Mary", Age = 32 };

			using (var tx = stateManager.CreateTransaction())
			{
				await dictionary.AddAsync(tx, john.Id, john);
				await dictionary.AddAsync(tx, jane.Id, jane);
				await dictionary.AddAsync(tx, mary.Id, mary);
				await tx.CommitAsync();
			}

			using (var tx = stateManager.CreateTransaction())
			{
				var enumerable = await dictionary.CreateIndexEnumerableAsync<int>(tx, "age", EnumerationMode.Ordered);
				var ages = (await enumerable.ToEnumerable()).ToArray();
				Assert.AreEqual(2, ages.Count());
				CollectionAssert.AreEqual(new[] { 25, 32 }, ages);

				await tx.CommitAsync();
			}
		}
	}
}
