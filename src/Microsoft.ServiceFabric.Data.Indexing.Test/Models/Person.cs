using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Data.Indexing.Test.Models
{
	public sealed class Person
	{
		public Guid Id { get; set; } = Guid.NewGuid();
		public string Name { get; set; }
		public int Age { get; set; }
		public Address Address { get; set; }

		public static Person CreateRandom(Random random)
		{
			return new Person
			{
				Name = new string((char)random.Next('a', 'z'), random.Next(5, 10)),
				Age = random.Next(20, 50),
				Address = Address.CreateRandom(random),
			};
		}
	}
}
