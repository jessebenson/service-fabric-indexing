using System;

namespace StoreService
{
	public class Product
	{
		public string Sku { get; set; }
		public string Name { get; set; }
		public string Category { get; set; }
		public string Description { get; set; }
		public double Price { get; set; }
		public int Quantity { get; set; }
	}
}
