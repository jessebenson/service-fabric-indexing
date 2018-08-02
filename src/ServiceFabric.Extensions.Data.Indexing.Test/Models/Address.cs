using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServiceFabric.Extensions.Data.Indexing.Persistent.Test.Models
{
	public sealed class Address
	{
		public string AddressLine1 { get; set; }
		public string AddressLine2 { get; set; }
		public string City { get; set; }
		public string State { get; set; }
		public int Zipcode { get; set; }

		public static Address CreateRandom(Random random)
		{
			return new Address
			{
				AddressLine1 = new string((char)random.Next('a', 'z'), random.Next(10, 20)),
				AddressLine2 = null,
				City = new string((char)random.Next('a', 'z'), random.Next(5, 10)),
				State = "WA",
				Zipcode = random.Next(10000, 99999),
			};
		}
	}
}
