# Microsoft.ServiceFabric.Data.Indexing

Service Fabric reliable collections that support automatic reverse indexing via filters and full-text search.

## Overview
This library adds IReliableIndexedDictionary, which wraps a standard IReliableDictionary and automatically creates and manages user-defined indexes on the data.  An IReliableIndexedDictionary is an IReliableDictionary, so you can use it with no changes to your usage - just how you create it.  This interface adds a few extra methods for filtering/searching using the indexes.

## Usage
1. Replace your calls to IReliableStateManager.GetOrAddAsync() with the extension methods IReliableStateManager.GetOrAddIndexedAsync().

```csharp
public class MyService : StatefulService
{
    public override async Task RunAsync(CancellationToken token)
    {
        var dictionary = await StateManager.GetOrAddIndexedAsync<string, Product>("products");
        
        // Use it like normal:
        using (var tx = StateManager.CreateTransaction())
        {
            await dictionary.SetAsync(tx, "red-polo", new Product { Name = "Red Polo", Description = "..." });
            await tx.CommitAsync();
        }
    }
}
```

2. Define your indexes (name and property), and pass those to IReliableStateManager.GetOrAddIndexedAsync().  Index names must be unique for a given reliable collection (they can be the same for different collections).

```csharp
public class MyService : StatefulService
{
    public override async Task RunAsync(CancellationToken token)
    {
        var dictionary = await StateManager.GetOrAddIndexedAsync<string, Product>("products",
            // Add a reverse index on the Product.Name property:
            new FilterableIndex<string, Product, string>("name", (k, v) => v.Name),
            // Add a full-text index on the Product.Description property:
            new SearchableIndex<string, Product>("description", (k, v) => v.Description));
    }
}
```

3. After adding data to the collection, use the FilterAsync() and SearchAsync() methods to quickly retrieve results.

```csharp
public class MyService : StatefulService
{
    public override async Task RunAsync(CancellationToken token)
    {
        var dictionary = await StateManager.GetOrAddIndexedAsync<string, Product>("products",
            // Add a reverse index on the Product.Name property:
            new FilterableIndex<string, Product, string>("name", (k, v) => v.Name),
            // Add a full-text index on the Product.Description property:
            new SearchableIndex<string, Product>("description", (k, v) => v.Description));
        
        // Add some data.
        using (var tx = StateManager.CreateTransaction())
        {
            await dictionary.SetAsync(tx, "red-polo", new Product { Name = "Red Polo", Description = "A red polo t-shirt." });
            await dictionary.SetAsync(tx, "red-skirt", new Product { Name = "Red Skirt", Description = "A long red skirt." });
            await dictionary.SetAsync(tx, "blue-skirt", new Product { Name = "Blue Skirt", Description = "A long blue skirt." });
            await tx.CommitAsync();
        }
        
        // Filter and Search the collection - return value is an IEnumerable<KeyValuePair<TKey, TValue>>.
        using (var tx = StateManager.CreateTransaction())
        {
            var results = await dictionary.FilterAsync(tx, "name", "Red Polo");
            // 'results' contains the "red-polo" Product.
            
            results = await dictionary.SearchAsync(tx, "red");
            // 'results' contains both "red-polo" and "red-skirt" products.
            
            results = await dictionary.SearchAsync(tx, "skirt");
            // 'results' contains both "red-skirt" and "blue-skirt" products.
            
            results = await dictionary.SearchAsync(tx, "long red");
            // 'results' contains all of "red-polo", "red-skirt", and "blue-skirt" products:
            // "red-polo" because "red" was found in the description.
            // "red-skirt" because "long" and "red" were found in the description.
            // "blue-skirt" because "long" was found in the description.
            
            await tx.CommitAsync();
        }
    }
}
```

## FilterableIndex
Filterable indexes define a reverse index on a given property.  Filtering will only return key-values that have exact matches for the property.

- Ideal for indexing on properties with small-sized values.
- The property value must be deterministic given a key and value, but need not be a property on either (it can be a composite value).  
- The property value must be a comparable type (IComparable<T> and IEquatable<T>).

```csharp
// constructor:
public FilterableIndex<TKey, TValue, TFilter>(string name, Func<TKey, TValue, TFilter> filter)

// index definition:
new FilterableIndex<string, Person, int>("age", (string key, Person value) => value.Age);

// usage:
IEnumerable<KeyValuePair<string, Person>> results = await dictionary.FilterAsync(tx, "age", 30);
```

## SearchableIndex

Searchable indexes define a full-text index on a given string property.  Searching is case-insensitive, and returns key-values that contain any of the full words from the search text.  The string property is split into words for indexing.  The search string is also split into words for searching.  The set of key-values that contain any word in the search text are returned (no duplicate keys will be returned).

- The property must be a string.
- The property value must be deterministic given a key and value, but need not be a property on either (it can be a composite value).
- All searchable indexes are used on SearchAsync(), unlike FilterAsync() where you specify the index.
- Each key that matches will be returned exactly once.
- Search is done through case-insensitive, exact word match (no stemming is done).  For example, "book" matches "The book" and "The Book", but not "The books".

```csharp
// constructor:
public SearchableIndex<TKey, TValue>(string name, Func<TKey, TValue, string> property)

// usage:
new SearchableIndex<string, Product>("description", (string key, Product value) => value.Description);

// usage:
IEnumerable<KeyValuePair<string, Product>> results = await dictionary.SearchAsync(tx, "search text here");
```
