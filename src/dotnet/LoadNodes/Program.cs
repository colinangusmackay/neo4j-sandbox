using System;
using System.Collections.Generic;
using System.IO;
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
            //LoadBadgeClass();
            //LoadBadges();
            CreateBadgesCsvFile();
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
            foreach (var row in GetRows("badges.xml").Where(r=>r.Attribute("UserId").Value == "8152"))//.Skip(10).Take(10))
            {
                var name = row.Attribute("Name").Value;
                var userId = Convert.ToInt32(row.Attribute("UserId").Value);
                var isTagBased = Convert.ToBoolean(row.Attribute("TagBased").Value);
                var badgeClassId = Convert.ToInt32(row.Attribute("Class").Value);
                if (isTagBased)
                {
                    name += " : " + classes.First(c => c.Id == badgeClassId).Name;
                }
                Console.WriteLine("Badge : {0}", name);

                var newBadge = new Badge
                {
                    Name = name,
                };

                var q =_client.Cypher
                    .Merge("(badge:Badge { Name: {name} })")
                    .OnCreate()
                    .Set("badge = {newBadge}")
                    .WithParams(new
                    {
                        name = newBadge.Name,
                        newBadge = newBadge
                    });
                Console.WriteLine("{0}", q.Query.QueryText);
                Console.WriteLine("{0}", q.Query.QueryParameters);
                q.ExecuteWithoutResults();

                q = _client.Cypher
                    .Match("(badge:Badge)", "(class:Class)")
                    .Where((Badge badge) => badge.Name == name)
                    .AndWhere((BadgeClass @class) => @class.Id == badgeClassId)
                    .CreateUnique("badge-[:IS_IN_CLASS]->class");
                Console.WriteLine("{0}", q.Query.QueryText);
                Console.WriteLine("{0}", q.Query.QueryParameters);
                q.ExecuteWithoutResults();

                q = _client.Cypher
                    .Match("(badge:Badge)", "(user:User)")
                    .Where((Badge badge) => badge.Name == name)
                    .AndWhere((User user) => user.Id == userId)
                    .Create("user-[:EARNED]->badge");
                Console.WriteLine("{0}", q.Query.QueryText);
                Console.WriteLine("{0}", q.Query.QueryParameters);
                q.ExecuteWithoutResults();

                Console.WriteLine(new String('-',72));
            }
        }

        static void CreateBadgesCsvFile()
        {
            using (FileStream fs = new FileStream("badges-batch4.csv", FileMode.Create, FileAccess.Write, FileShare.None))
            {
                using (StreamWriter writer = new StreamWriter(fs))
                {
                    int count = 0;
                    writer.WriteLine("UserId,Name,ClassId,TagBased,TagName");
                    foreach (var row in GetRows("badges.xml")
                        .Skip(20).Where(r => r.Attribute("UserId").Value == "8152"))
//                        .Where(r => Convert.ToInt32(r.Attribute("UserId").Value) >= 10000)
//                        .Where(r => Convert.ToInt32(r.Attribute("UserId").Value) < 1000000))
                    {
                        var name = row.Attribute("Name").Value;
                        var userId = Convert.ToInt32(row.Attribute("UserId").Value);
                        var isTagBased = Convert.ToBoolean(row.Attribute("TagBased").Value);
                        var badgeClassId = Convert.ToInt32(row.Attribute("Class").Value);
                        var tagName = "";
                        if (isTagBased)
                        {
                            tagName = name;
                            name += " : " + classes.First(c => c.Id == badgeClassId).Name;
                        }
                        writer.WriteLine("{0},\"{1}\",{2},{3},\"{4}\"",userId,name,badgeClassId,isTagBased?1:0,tagName);
                        count++;
                        if (count%10000 == 0)
                            Console.WriteLine("Processed {0} rows", count);
                    }
                }
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
