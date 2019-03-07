using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Data.Mocks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ServiceFabric.Extensions.Data.Indexing.Persistent.Test.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ServiceFabric.Extensions.Data.Indexing.Persistent.Test
{
    [TestClass]
    public class FilterableIndexTests
    {
        private static readonly Random Random = new Random();

        [TestMethod]
        public async Task StringFilter_Add_WithNullString()
        {
            var stateManager = new MockReliableStateManager();
            var dictionary = await stateManager.GetOrAddIndexedAsync("test",
                new FilterableIndex<Guid, Person, string>("name", (k, p) => p.Name, true));

            // Add person using normal IReliableDictionary APIs.  This should update the index as well.
            var john = new Person { Name = "John" };
            var noname = new Person { Age = -1 };
            using (var tx = stateManager.CreateTransaction())
            {
                await dictionary.AddAsync(tx, john.Id, john);
                await dictionary.AddAsync(tx, noname.Id, noname);
                await tx.CommitAsync();
            }

            using (var tx = stateManager.CreateTransaction())
            {
                // Search the index for this person's name.  This should return the person we added above.
                var temp = await dictionary.FilterAsync(tx, "name", "John");
                var result = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(1, result.Count());
                Assert.AreEqual(john.Id, result.First().Key);
                Assert.AreSame("John", result.First().Value.Name);

                // Search the index for the wrong name.  This should not return any results.
                temp = await dictionary.FilterAsync<string>(tx, "name", null);
                var nobody = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(1, nobody.Count());
                Assert.AreEqual(noname.Id, result.First().Key);
                Assert.AreEqual(-1, result.First().Value.Age);


                await tx.CommitAsync();
            }
        }

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
                var temp = await dictionary.FilterAsync(tx, "name", "John");
                var result = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(1, result.Count());
                Assert.AreEqual(john.Id, result.First().Key);
                Assert.AreSame(john, result.First().Value);

                // Search the index for the wrong name.  This should not return any results.
                temp = await dictionary.FilterAsync(tx, "name", "Jane");
                var nobody = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
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
                var temp = await dictionary.FilterAsync(tx, "name", "John");
                var result = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
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
                var temp = await dictionary.FilterAsync(tx, "name", "John");
                var result = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(0, result.Count());

                // Search the index for Jane.  This should return the Jane person.
                temp = await dictionary.FilterAsync(tx, "name", "Jane");
                result = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
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
                var temp = await dictionary.FilterAsync(tx, "name", "John");
                var results = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(2, results.Count());
                CollectionAssert.Contains(results.Select(x => x.Value).ToArray(), john1);
                CollectionAssert.Contains(results.Select(x => x.Value).ToArray(), john2);

                // Search the index for the wrong name.  This should not return any results.
                temp = await dictionary.FilterAsync(tx, "name", "Jane");
                var nobody = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(0, nobody.Count());

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
                var temp = await dictionary.RangeFilterAsync(tx, "age", 0, RangeFilterType.Inclusive, 10, RangeFilterType.Inclusive);
                var results = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(0, results.Count());

                // Range filter - range too high.
                temp = await dictionary.RangeFilterAsync(tx, "age", 70, RangeFilterType.Inclusive, 100, RangeFilterType.Inclusive);
                results = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(0, results.Count());

                // Range filter - fully included (order is important).
                temp = await dictionary.RangeFilterAsync(tx, "age", 0, RangeFilterType.Inclusive, 100, RangeFilterType.Inclusive);
                results = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(3, results.Count());
                CollectionAssert.AreEqual(new[] { jane, john, mary }, results.Select(x => x.Value).ToArray());

                // Range filter - partially included.
                temp = await dictionary.RangeFilterAsync(tx, "age", 30, RangeFilterType.Inclusive, 40, RangeFilterType.Inclusive);
                results = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(2, results.Count());
                CollectionAssert.AreEqual(new[] { john, mary }, results.Select(x => x.Value).ToArray());

                // Range filter - partially included, start overlaps.
                temp = await dictionary.RangeFilterAsync(tx, "age", 32, RangeFilterType.Inclusive, 40, RangeFilterType.Inclusive);
                results = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(2, results.Count());
                CollectionAssert.AreEqual(new[] { john, mary }, results.Select(x => x.Value).ToArray());

                // Range filter - partially included, end overlaps.
                temp = await dictionary.RangeFilterAsync(tx, "age", 30, RangeFilterType.Inclusive, 35, RangeFilterType.Inclusive);
                results = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(2, results.Count());
                CollectionAssert.AreEqual(new[] { john, mary }, results.Select(x => x.Value).ToArray());

                // Range filter - partially included, in the middle.
                temp = await dictionary.RangeFilterAsync(tx, "age", 30, RangeFilterType.Inclusive, 33, RangeFilterType.Inclusive);
                results = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(1, results.Count());
                CollectionAssert.AreEqual(new[] { john }, results.Select(x => x.Value).ToArray());

                // Range filter - partially included, exclusive.
                temp = await dictionary.RangeFilterAsync(tx, "age", 30, RangeFilterType.Exclusive, 35, RangeFilterType.Exclusive);
                results = new List<KeyValuePair<Guid, Person>>(await temp.ToEnumerable());
                Assert.AreEqual(1, results.Count());
                var singleActual = results.Select(x => x.Value).First();
                Assert.IsTrue(singleActual == john);

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
