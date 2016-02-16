using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using Neo4jClient;

namespace LoadNodes
{
    class Tag
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    class User
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int UpVotes { get; set; }
        public int DownVotes { get; set; }
        public int Reputation { get; set; }
    }

    class Badge
    {
        public string Name { get; set; }
    }

    class BadgeClass
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    class Program
    {
        static BadgeClass[] classes = new[]
            {
                new BadgeClass {Id = 1, Name = "Gold"},
                new BadgeClass {Id = 2, Name = "Silver"},
                new BadgeClass {Id = 3, Name = "Bronze"}
            };

        private static GraphClient _client;
        static void Main(string[] args)
        {
            _client = new GraphClient(new Uri("http://localhost:7474/db/data"), "neo4j", "neo4jPassword");
            _client.Connect();
            //LoadTags();
            //LoadUsers();
            LoadBadgeClass();
            LoadBadges();
            Console.WriteLine("Done!");
            Console.ReadLine();
        }

        static void LoadTags()
        {
            foreach (var row in GetRows("tags.xml"))
            {
                var id = row.Attribute("Id");
                var tagName = row.Attribute("TagName");
                Console.WriteLine("Tag({0}) : {1}", id.Value, tagName.Value);
                var tag = new Tag {Id = Convert.ToInt32(id.Value), Name = tagName.Value};
                _client.Cypher
                    .Create("(tag:Tag {newTag})")
                    .WithParam("newTag", tag)
                    .ExecuteWithoutResults();
            }
        }

        static void LoadUsers()
        {
            foreach (var row in GetRows("users.xml"))
            {
                var id = Convert.ToInt32(row.Attribute("Id").Value);
                var displayName = row.Attribute("DisplayName").Value;
                Console.WriteLine("User({0}) : {1}", id, displayName);

                var user = new User
                {
                    Id = id, 
                    Name = displayName,
                    UpVotes = Convert.ToInt32(row.Attribute("UpVotes").Value),
                    DownVotes = Convert.ToInt32(row.Attribute("DownVotes").Value),
                    Reputation = Convert.ToInt32(row.Attribute("Reputation").Value)
                };
                _client.Cypher
                    .Create("(user:User {newUser})")
                    .WithParam("newUser", user)
                    .ExecuteWithoutResults();
            }
        }

        static void LoadBadgeClass()
        {
            foreach (var badgeClass in classes)
            {
                _client.Cypher
                    .Create("(class:Class {newClass})")
                    .WithParam("newClass", badgeClass)
                    .ExecuteWithoutResults();
            }
        }

        static void LoadBadges()
        {
            foreach (var row in GetRows("badges.xml"))
            {
                var name = row.Attribute("Name").Value;
                var isTagBased = Convert.ToBoolean(row.Attribute("TagBased").Value);
                var badgeClassId = Convert.ToInt32(row.Attribute("Class").Value);
                if (isTagBased)
                {
                    name += " : " + classes.First(c => c.Id == badgeClassId).Name;
                }
                Console.WriteLine("Badge : {0}", name);

                var badge = new Badge
                {
                    Name = name,
                };

                _client.Cypher
                    .Merge("(badge:Badge { Name: {name} })")
                    .OnCreate()
                    .Set("badge = {newBadge}")
                    .WithParams(new
                    {
                        name = badge.Name,
                        badge
                    })
                    .ExecuteWithoutResults(); 
            }
        }

        static IEnumerable<XElement> GetRows(string xmlFile)
        {
            using (XmlReader reader = XmlReader.Create(xmlFile))
            {
                reader.MoveToContent();
                while (reader.Read())
                {
                    if (reader.NodeType != XmlNodeType.Element && reader.Name != "row")
                        continue;

                    var row = XNode.ReadFrom(reader) as XElement;
                    yield return row;
                }
            }
        } 
    }
}
