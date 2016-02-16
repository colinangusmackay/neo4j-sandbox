"use strict";

var neo4j = require("neo4j"),
    XmlStream = require("xml-stream"),
    fs = require("fs"),
    helpers = require("./helpers"),
    dataDir = "../data/",
    db = new neo4j.GraphDatabase({
        url:"http://localhost:7474",
        auth: {username:"neo4j", password:"myPassword"},
        headers:{},
        proxy: null,
        agent: null
    })


function loadBadges(){
    return new Promise(function(resolve, reject){
        let stream = fs.createReadStream(dataDir+"Badges.xml"),
            xml = new XmlStream(stream),
            count = 0;

        xml.collect('row');
        xml.on('endElement: row', function(item) {
            if (item["$"].UserId === "8152"){
                console.log(item["$"]);
            }
            count++;
            if ((count % 10000) === 0){
                console.log("Processed "+count);
            }
        });

        xml.on("end", function(){
            console.log("Done Tags!");
            stream.close();
            resolve(count);
        });
    });
}


function loadTags(){
    return new Promise(function(resolve, reject){
        let stream = fs.createReadStream(dataDir+"Tags.xml"),
            xml = new XmlStream(stream),
            count = 0;

        xml.collect('row');
        xml.on('endElement: row', function(item) {
            console.log(item["$"].TagName);
            count++;
        });

        xml.on("end", function(){
            console.log("Done Tags!");
            stream.close();
            resolve(count);
        });
    });
}



helpers.async.run(function*(){

    var badges = yield loadBadges();
    console.log("Processed "+badges+ " badges.");

    process.exit();
});


