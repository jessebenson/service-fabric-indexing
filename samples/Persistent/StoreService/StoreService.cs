using System;
using System.Fabric;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Data.Indexing.Persistent;

namespace StoreService
{
	/// <summary>
	/// An instance of this class is created for each service replica by the Service Fabric runtime.
	/// </summary>
	internal sealed class StoreService : StatefulService
	{
		public StoreService(StatefulServiceContext context)
			: base(context)
		{ }

		/// <summary>
		/// This is the main entry point for your service replica.
		/// This method executes when this replica of your service becomes primary and has write status.
		/// </summary>
		/// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
		protected override async Task RunAsync(CancellationToken cancellationToken)
		{
			var products = await StateManager.GetOrAddIndexedAsync<string, Product>("products",
				new FilterableIndex<string, Product, string>("category", (k, v) => v.Category),
				new FilterableIndex<string, Product, double>("price", (k, v) => v.Price),
				new FilterableIndex<string, Product, int>("quantity", (k, v) => v.Quantity),
				new SearchableIndex<string, Product>("category", (k, v) => v.Category),
				new SearchableIndex<string, Product>("description", (k, v) => v.Description));

			// Add some products.
			using (var tx = StateManager.CreateTransaction())
			{
				await products.SetAsync(tx, "sku-0", new Product { Sku = "sku-0", Name = "Red Polo", Category = "Tops", Description = "This is a light red polo shirt.", Price = 24.99, Quantity = 10 });
				await products.SetAsync(tx, "sku-1", new Product { Sku = "sku-1", Name = "Blue Sweater", Category = "Tops", Description = "This is a heavy blue sweater.", Price = 49.99, Quantity = 20 });
				await products.SetAsync(tx, "sku-2", new Product { Sku = "sku-2", Name = "White Skirt", Category = "Bottoms", Description = "This is a long white skirt.", Price = 29.99, Quantity = 15 });
				await products.SetAsync(tx, "sku-3", new Product { Sku = "sku-3", Name = "Blue Jeans", Category = "Bottoms", Description = "This is a pair of blue jeans.", Price = 19.99, Quantity = 100 });

				await tx.CommitAsync();
			}

			// Query the products.
			using (var tx = StateManager.CreateTransaction())
			{
				// Returns [sku-0, sku-1]:
				var tops = await products.FilterAsync(tx, "category", "Tops");

				// Returns [sku-2, sku-3]:
				var bottoms = await products.FilterAsync(tx, "category", "Bottoms");

				// Returns [sku-1, sku-3]:
				var blue = await products.SearchAsync(tx, "blue");

				// Returns [sku-0, sku-2]:
				var midPrice = await products.RangeFilterAsync<double>(tx, "price", 20, 30);

				// Returns [sku-0, sku-2, sku-1]:
				var lowQuantity = await products.RangeFilterAsync<int>(tx, "quantity", 0, 20);

				await tx.CommitAsync();
			}
		}
	}
}
