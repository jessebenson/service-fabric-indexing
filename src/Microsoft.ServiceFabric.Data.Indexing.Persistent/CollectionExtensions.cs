using System;
using System.Collections.Generic;

namespace Microsoft.ServiceFabric.Data.Indexing.Persistent
{
	internal static class CollectionExtensions
	{
		public static void AddRange<T>(this HashSet<T> set, IEnumerable<T> items)
		{
			foreach (var item in items)
			{
				set.Add(item);
			}
		}

		public static T[] CopyAndAdd<T>(this T[] array, T value)
		{
			if (array == null)
				throw new ArgumentNullException(nameof(array));

			var newArray = new T[array.Length + 1];
			Array.Copy(array, newArray, array.Length);
			newArray[array.Length] = value;
			return newArray;
		}

		public static T[] CopyAndRemove<T>(this T[] array, T value)
		{
			if (array == null)
				throw new ArgumentNullException(nameof(array));

			int index = Array.IndexOf(array, value);
			if (index < 0)
				throw new KeyNotFoundException();

			var newArray = new T[array.Length - 1];
			Array.Copy(array, 0, newArray, 0, index);
			Array.Copy(array, index + 1, newArray, index, newArray.Length - index);
			return newArray;
		}
	}
}
