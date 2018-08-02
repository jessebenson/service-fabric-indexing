using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;

namespace ServiceFabric.Extensions.Data.Indexing.Persistent
{
    sealed class DictionaryFetchAsyncEnumerable<TKey, TValue> : IAsyncEnumerable<KeyValuePair<TKey, TValue>>
        where TKey : IEquatable<TKey>, IComparable<TKey>
    {
        private readonly ITransaction Tx;
        private readonly IReliableIndexedDictionary<TKey, TValue> Dictionary;
        private readonly IEnumerable<TKey> Keys;
        private readonly TimeSpan Timeout;
        private readonly CancellationToken Token;


        public DictionaryFetchAsyncEnumerable(ITransaction tx, IReliableIndexedDictionary<TKey, TValue> dictionary, IEnumerable<TKey> keys, TimeSpan timeout, CancellationToken token)
        {
            Tx = tx;
            Dictionary = dictionary;
            Keys = keys;
            Timeout = timeout;
            Token = token;
        }

        public IAsyncEnumerator<KeyValuePair<TKey, TValue>> GetAsyncEnumerator()
        {
            return new DictionaryFetchAsyncEnumerator<TKey, TValue>(Tx, Dictionary, Keys, Timeout, Token);
        }
    }

    internal class DictionaryFetchAsyncEnumerator<TKey, TValue> : IAsyncEnumerator<KeyValuePair<TKey, TValue>>
        where TKey : IEquatable<TKey>, IComparable<TKey>
    {
        private readonly ITransaction Tx;
        private readonly IReliableIndexedDictionary<TKey, TValue> Dictionary;
        private readonly IEnumerator<TKey> Keys;
        private readonly TimeSpan Timeout;
        private readonly CancellationToken Token;

        public DictionaryFetchAsyncEnumerator(ITransaction tx, IReliableIndexedDictionary<TKey, TValue> dictionary, IEnumerable<TKey> keys, TimeSpan timeout, CancellationToken token)
        {
            Tx = tx;
            Dictionary = dictionary;
            Keys = keys.GetEnumerator();
            Timeout = timeout;
            Token = token;
        }

        public KeyValuePair<TKey, TValue> Current { get; private set; }

        public void Dispose()
        {
            Keys.Dispose();
        }

        public async Task<bool> MoveNextAsync(CancellationToken cancellationToken)
        {
            if (Keys.MoveNext())
            {
                var result = await Dictionary.TryGetValueAsync(Tx, Keys.Current, Timeout, Token).ConfigureAwait(false);
                if (!result.HasValue)
                    return await MoveNextAsync(cancellationToken);

                // TODO: since we're doing snapshot reads, the value may have changed since we read the index.  We should validate the key-value still match the filter/search/etc.
                // Note: In queryable this is still done because the OData are still applied to the remaining KeyValue set
                Current = new KeyValuePair<TKey, TValue>(Keys.Current, result.Value);
                return true;
            }
            return false;
        }

        public void Reset()
        {
            Keys.Reset();
        }
    }
}
