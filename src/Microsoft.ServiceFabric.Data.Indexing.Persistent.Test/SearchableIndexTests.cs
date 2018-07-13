using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Indexing.Persistent.Test.Models;
using Microsoft.ServiceFabric.Data.Mocks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.ServiceFabric.Data.Indexing.Persistent.Test
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
				var johnSearch = new List<KeyValuePair<Guid, Person>>(await dictionary.SearchAsync(tx, "John").Result.ToEnumerable());
                Assert.AreEqual(1, johnSearch.Count());
				Assert.AreEqual(john.Id, johnSearch.First().Key);
				Assert.AreSame(john, johnSearch.First().Value);

				var janeSearch = new List<KeyValuePair<Guid, Person>>(await dictionary.SearchAsync(tx, "Jane").Result.ToEnumerable());
                Assert.AreEqual(1, janeSearch.Count());
				Assert.AreEqual(jane.Id, janeSearch.First().Key);
				Assert.AreSame(jane, janeSearch.First().Value);

				// Search the index for the last name.  This should return both.
				var doeSearch = new List<KeyValuePair<Guid, Person>>(await dictionary.SearchAsync(tx, "Doe").Result.ToEnumerable());
                Assert.AreEqual(2, doeSearch.Count());
				CollectionAssert.Contains(doeSearch.Select(x => x.Value).ToArray(), john);
				CollectionAssert.Contains(doeSearch.Select(x => x.Value).ToArray(), jane);

				// Search the index for the last name as lower-case.  This should also return both.
				doeSearch = new List<KeyValuePair<Guid, Person>>(await dictionary.SearchAsync(tx, "doe").Result.ToEnumerable());
                Assert.AreEqual(2, doeSearch.Count());
				CollectionAssert.Contains(doeSearch.Select(x => x.Value).ToArray(), john);
				CollectionAssert.Contains(doeSearch.Select(x => x.Value).ToArray(), jane);

				// Search the index for a non-existent string.
				var nobody = new List<KeyValuePair<Guid, Person>>(await dictionary.SearchAsync(tx, "unknown").Result.ToEnumerable());
                Assert.AreEqual(0, nobody.Count());

				// Search the index for the last name as lower-case with a count limit.  This should return one.
				doeSearch = new List<KeyValuePair<Guid, Person>>(await dictionary.SearchAsync(tx, "doe", count: 1).Result.ToEnumerable());
                Assert.AreEqual(1, doeSearch.Count());
				var singleActual = doeSearch.Select(x => x.Value).First();
				Assert.IsTrue(singleActual == john || singleActual == jane);

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
				var johnsonSearch = new List<KeyValuePair<Guid, Person>>(await dictionary.SearchAsync(tx, "Johnson").Result.ToEnumerable());
                Assert.AreEqual(2, johnsonSearch.Count());
				CollectionAssert.Contains(johnsonSearch.Select(x => x.Value).ToArray(), mark);
				CollectionAssert.Contains(johnsonSearch.Select(x => x.Value).ToArray(), jane);

				// Search for 'Main' should only return Mark (address match).
				var mainSearch = new List<KeyValuePair<Guid, Person>>(await dictionary.SearchAsync(tx, "Main").Result.ToEnumerable());
                Assert.AreEqual(1, mainSearch.Count());
				CollectionAssert.Contains(mainSearch.Select(x => x.Value).ToArray(), mark);

				// Search for 'Mark and Jane' should return both people ('and' word should be ignored).
				var markJaneSearch = new List<KeyValuePair<Guid, Person>>(await dictionary.SearchAsync(tx, "Mark and Jane").Result.ToEnumerable());
                Assert.AreEqual(2, markJaneSearch.Count());
				CollectionAssert.Contains(markJaneSearch.Select(x => x.Value).ToArray(), mark);
				CollectionAssert.Contains(markJaneSearch.Select(x => x.Value).ToArray(), jane);

				// Search for 'Street' should not return anybody.
				var streetSearch = new List<KeyValuePair<Guid, Person>>(await dictionary.SearchAsync(tx, "Street").Result.ToEnumerable());
				Assert.AreEqual(0, streetSearch.Count());

				await tx.CommitAsync();
			}
		}
	}
}
