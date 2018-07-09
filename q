[1mdiff --cc src/Microsoft.ServiceFabric.Data.Indexing.Persistent/ReliableIndexedDictionary.cs[m
[1mindex f12738c,d5b3b64..0000000[m
[1m--- a/src/Microsoft.ServiceFabric.Data.Indexing.Persistent/ReliableIndexedDictionary.cs[m
[1m+++ b/src/Microsoft.ServiceFabric.Data.Indexing.Persistent/ReliableIndexedDictionary.cs[m
[36m@@@ -334,22 -346,28 +334,22 @@@[m [mnamespace Microsoft.ServiceFabric.Data.[m
  			return await GetAllAsync(tx, keys, timeout, token).ConfigureAwait(false);[m
  		}[m
  [m
[31m -		public Task<IEnumerable<KeyValuePair<TKey, TValue>>> RangeFilterAsync<TFilter>(ITransaction tx, string index, TFilter start, TFilter end)[m
[31m -			where TFilter : IComparable<TFilter>, IEquatable<TFilter>[m
[31m -		{[m
[31m -			return RangeFilterAsync(tx, index, start, end, int.MaxValue, DefaultTimeout, CancellationToken.None);[m
[31m -		}[m
[31m -[m
[31m -		public Task<IEnumerable<KeyValuePair<TKey, TValue>>> RangeFilterAsync<TFilter>(ITransaction tx, string index, TFilter start, TFilter end, int count)[m
[32m +		public Task<IEnumerable<KeyValuePair<TKey, TValue>>> RangeFilterAsync<TFilter>(ITransaction tx, string index, TFilter startFilter, RangeFilterType startType, TFilter endFilter, RangeFilterType endType)[m
  			where TFilter : IComparable<TFilter>, IEquatable<TFilter>[m
  		{[m
[31m -			return RangeFilterAsync(tx, index, start, end, count, DefaultTimeout, CancellationToken.None);[m
[32m +			return RangeFilterAsync(tx, index, startFilter, startType, endFilter, endType, DefaultTimeout, CancellationToken.None);[m
  		}[m
  [m
[31m -		public async Task<IEnumerable<KeyValuePair<TKey, TValue>>> RangeFilterAsync<TFilter>(ITransaction tx, string indexName, TFilter start, TFilter end, int count, TimeSpan timeout, CancellationToken token)[m
[32m +		public async Task<IEnumerable<KeyValuePair<TKey, TValue>>> RangeFilterAsync<TFilter>(ITransaction tx, string indexName, TFilter startFilter, RangeFilterType startType, TFilter endFilter, RangeFilterType endType, TimeSpan timeout, CancellationToken token)[m
  			where TFilter : IComparable<TFilter>, IEquatable<TFilter>[m
[31m -		{[m
[31m -			// Find the index.[m
[31m -			var index = GetFilterableIndex<TFilter>(indexName);[m
[31m -[m
[31m -			// Find the keys that fall within this range (inclusively).[m
[31m -			var keys = await index.RangeFilterAsync(tx, start, RangeFilterType.INCLUSIVE, end, RangeFilterType.INCLUSIVE, count, token).ConfigureAwait(false);[m
[31m -[m
[32m +		{[m
[32m +			// Find the index.[m
[32m +			var index = GetFilterableIndex<TFilter>(indexName);[m
[32m +[m
[32m +			// Find the keys that fall within this range (inclusively or exclusively).[m
[32m +			var keys = await index.RangeFilterAsync(tx, startFilter, startType, endFilter, endType, token).ConfigureAwait(false);[m
[32m +[m
[31m- 			// Get the rows that match this filter.[m
[32m+ 			// Get the rows that match this filter.[m
  			return await GetAllAsync(tx, keys, timeout, token).ConfigureAwait(false);[m
  		}[m
  [m
[36m@@@ -384,28 -402,28 +384,28 @@@[m
  		{[m
  			var results = new List<KeyValuePair<TKey, TValue>>();[m
  			foreach (var key in keys)[m
[31m -			{[m
[31m -				// Since we're doing snapshot reads to get the set of keys, the key may get removed by the time we try to read it.[m
[32m +			{[m
[32m +				// Since we're doing snapshot reads to get the set of keys, the key may get removed by the time we try to read it.[m
  				var result = await _dictionary.TryGetValueAsync(tx, key, timeout, token).ConfigureAwait(false);[m
  				if (!result.HasValue)[m
[31m -					continue;[m
[31m -[m
[31m -				// TODO: since we're doing snapshot reads, the value may have changed since we read the index.  We should validate the key-value still match the filter/search/etc.[m
[31m -[m
[32m +					continue;[m
[32m +[m
[32m +				// TODO: since we're doing snapshot reads, the value may have changed since we read the index.  We should validate the key-value still match the filter/search/etc.[m
[32m +				// Note: In queryable this is still done because the OData are still applied to the remaining KeyValue set[m
  				results.Add(new KeyValuePair<TKey, TValue>(key, result.Value));[m
[31m -			}[m
[31m -[m
[31m -			return Enumerable.AsEnumerable(results);[m
[32m +			}[m
[32m +[m
[32m +			return results;[m
  		}[m
  [m
[31m- 		private FilterableIndex<TKey, TValue, TFilter> GetFilterableIndex<TFilter>(string indexName)[m
[32m+ 		public FilterableIndex<TKey, TValue, TFilter> GetFilterableIndex<TFilter>(string indexName)[m
  			where TFilter : IComparable<TFilter>, IEquatable<TFilter>[m
[31m -		{[m
[31m -			// Find the index.[m
[32m +		{[m
[32m +			// Find the index.[m
  			if (!_filterIndexes.TryGetValue(indexName, out IIndexDefinition<TKey, TValue> definition))[m
[31m -				throw new KeyNotFoundException($"Index '{indexName}' not found.");[m
[31m -[m
[31m -			// Ensure the index is of the correct type.[m
[32m +				throw new KeyNotFoundException($"Index '{indexName}' not found.");[m
[32m +[m
[32m +			// Ensure the index is of the correct type.[m
  			var index = definition as FilterableIndex<TKey, TValue, TFilter>;[m
  			if (index == null)[m
  				throw new InvalidCastException($"Index '{indexName}' is not a filterable index of this type.");[m
[1mdiff --cc src/Microsoft.ServiceFabric.Data.Indexing.Persistent/packages.config[m
[1mindex 3b9fd6b,defc0ec..0000000[m
[1m--- a/src/Microsoft.ServiceFabric.Data.Indexing.Persistent/packages.config[m
[1m+++ b/src/Microsoft.ServiceFabric.Data.Indexing.Persistent/packages.config[m
[36m@@@ -1,10 -1,8 +1,10 @@@[m
[31m- ﻿<?xml version="1.0" encoding="utf-8"?>[m
[31m -﻿<?xml version="1.0" encoding="utf-8"?>[m
[31m -<packages>[m
[31m -  <package id="Microsoft.ServiceFabric" version="6.0.232" targetFramework="net461" />[m
[31m -  <package id="Microsoft.ServiceFabric.Data" version="2.8.232" targetFramework="net461" />[m
[31m -  <package id="Microsoft.ServiceFabric.Diagnostics.Internal" version="2.8.232" targetFramework="net461" />[m
[31m -  <package id="Microsoft.ServiceFabric.Services" version="2.8.232" targetFramework="net461" />[m
[32m++<?xml version="1.0" encoding="utf-8"?>[m
[32m +<packages>[m
[32m +  <package id="Microsoft.ServiceFabric" version="6.2.283" targetFramework="net461" />[m
[32m +  <package id="Microsoft.ServiceFabric.Data" version="3.1.283" targetFramework="net461" />[m
[32m +  <package id="Microsoft.ServiceFabric.Data.Extensions" version="1.3.283" targetFramework="net461" />[m
[32m +  <package id="Microsoft.ServiceFabric.Data.Interfaces" version="3.1.283" targetFramework="net461" />[m
[32m +  <package id="Microsoft.ServiceFabric.Diagnostics.Internal" version="3.1.283" targetFramework="net461" />[m
[32m +  <package id="Microsoft.ServiceFabric.Services" version="3.1.283" targetFramework="net461" />[m
[31m-   <package id="System.Linq.Dynamic" version="1.0.7" targetFramework="net461" />[m
[32m+   <package id="System.Linq.Dynamic" version="1.0.7" targetFramework="net461" />[m
  </packages>[m
