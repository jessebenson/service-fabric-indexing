using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.ServiceFabric.Data.Indexing.Test.Mocks;
using Microsoft.ServiceFabric.Data.Indexing.Test.Models;

namespace Microsoft.ServiceFabric.Data.Indexing.Test
{
	[TestClass]
	public class SearchableIndexTests
	{
		[TestMethod]
		public async Task SingleSearchIndex()
		{
			var stateManager = new MockReliableStateManager();
			var dictionary = await stateManager.GetOrAddIndexedAsync("test",
				new SearchableIndex<Guid, Person>("name", (k, p) => p.Name));

			// Add person using normal IReliableDictionary APIs.  This should update the index as well.
			var john = new Person { Name = "John Doe" };
			var jane = new Person { Name = "Jane Doe" };

			using (var tx = stateManager.CreateTransaction())
			{
				await dictionary.AddAsync(tx, john.Id, john);
				await dictionary.AddAsync(tx, jane.Id, jane);
				await tx.CommitAsync();
			}

			using (var tx = stateManager.CreateTransaction())
			{
				// Search by first names.  This should return the respective people we added above.
				var johnSearch = await dictionary.SearchAsync(tx, "John");
				Assert.AreEqual(1, johnSearch.Count());
				Assert.AreEqual(john.Id, johnSearch.First().Key);
				Assert.AreSame(john, johnSearch.First().Value);

				var janeSearch = await dictionary.SearchAsync(tx, "Jane");
				Assert.AreEqual(1, janeSearch.Count());
				Assert.AreEqual(jane.Id, janeSearch.First().Key);
				Assert.AreSame(jane, janeSearch.First().Value);

				// Search the index for the last name.  This should return both.
				var doeSearch = await dictionary.SearchAsync(tx, "Doe");
				Assert.AreEqual(2, doeSearch.Count());
				CollectionAssert.Contains(doeSearch.Select(x => x.Value).ToArray(), john);
				CollectionAssert.Contains(doeSearch.Select(x => x.Value).ToArray(), jane);

				// Search the index for the last name as lower-case.  This should also return both.
				doeSearch = await dictionary.SearchAsync(tx, "doe");
				Assert.AreEqual(2, doeSearch.Count());
				CollectionAssert.Contains(doeSearch.Select(x => x.Value).ToArray(), john);
				CollectionAssert.Contains(doeSearch.Select(x => x.Value).ToArray(), jane);

				// Search the index for a non-existent string.
				var nobody = await dictionary.SearchAsync(tx, "unknown");
				Assert.AreEqual(0, nobody.Count());

				await tx.CommitAsync();
			}
		}

		[TestMethod]
		public async Task MultipleSearchIndexes()
		{
			var stateManager = new MockReliableStateManager();
			var dictionary = await stateManager.GetOrAddIndexedAsync("test",
				new SearchableIndex<Guid, Person>("name", (k, p) => p.Name),
				new SearchableIndex<Guid, Person>("address", (k, p) => p.Address.AddressLine1));

			// Add person using normal IReliableDictionary APIs.  This should update the index as well.
			var mark = new Person { Name = "Mark Johnson", Address = new Address { AddressLine1 = "123 Main St." } };
			var jane = new Person { Name = "Jane Doe", Address = new Address { AddressLine1 = "456 Johnson St." } };

			using (var tx = stateManager.CreateTransaction())
			{
				await dictionary.AddAsync(tx, mark.Id, mark);
				await dictionary.AddAsync(tx, jane.Id, jane);
				await tx.CommitAsync();
			}

			using (var tx = stateManager.CreateTransaction())
			{
				// Search for 'Johnson' should return both people (Mark for name match, and Jane for address match).
				var johnsonSearch = await dictionary.SearchAsync(tx, "Johnson");
				Assert.AreEqual(2, johnsonSearch.Count());
				CollectionAssert.Contains(johnsonSearch.Select(x => x.Value).ToArray(), mark);
				CollectionAssert.Contains(johnsonSearch.Select(x => x.Value).ToArray(), jane);

				// Search for 'Main' should only return Mark (address match).
				var mainSearch = await dictionary.SearchAsync(tx, "Main");
				Assert.AreEqual(1, mainSearch.Count());
				CollectionAssert.Contains(mainSearch.Select(x => x.Value).ToArray(), mark);

				// Search for 'Mark and Jane' should return both people ('and' word should be ignored).
				var markJaneSearch = await dictionary.SearchAsync(tx, "Mark and Jane");
				Assert.AreEqual(2, markJaneSearch.Count());
				CollectionAssert.Contains(markJaneSearch.Select(x => x.Value).ToArray(), mark);
				CollectionAssert.Contains(markJaneSearch.Select(x => x.Value).ToArray(), jane);

				// Search for 'Street' should not return anybody.
				var streetSearch = await dictionary.SearchAsync(tx, "Street");
				Assert.AreEqual(0, streetSearch.Count());

				await tx.CommitAsync();
			}
		}
	}
}
