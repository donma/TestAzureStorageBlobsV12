using System;
using System.Collections.Generic;
using System.Text;

namespace TestAzureStorageBlobs
{
    public class User
    {
        public string Id { get; set; }
        public string Name { get; set; }

        public int Age { get; set; }

        public DateTime Create { get; set; }

        public List<User> Friends { get; set; }

        public User() {
            Friends = new List<User>();
        }
    }
}
