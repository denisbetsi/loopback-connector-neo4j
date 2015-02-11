'use strict';

var GraphDatabase = require('neo4j').GraphDatabase;

exports.initialize = function (dataSource, cb) {
    var settings = dataSource.settings;
    var db = new GraphDatabase(settings.neo4j_url);
    dataSource.connector = new Neo4j(db);
    dataSource.db = db;
    cb && cb();
}

/**
 * @constructor
 * @param {Object} db
 */
function Neo4j (db) {
    this.db = db;
}

var neo4j = Neo4j.prototype;

neo4j.create = function (model, data, cb) {
    var query = 'create (n:%node% {data}) return n'
        .replace('%node%', model);
    var params = { data: data };

    this.db.query(query, params, function (err, results) {
        if (err) return cb(err);
        cb(null, results[0][model]);
    });
}

neo4j.save = function (model, data, cb) {
    cb();
}

neo4j.all = function (model, filter, cb) {
    var keys = Object.keys(filter);
   
    var keyToSymbol = function (key) {
        return key + ':{filter}.' + key;
    }
   
    // Build filter which supports IN query
    var newfilter = "";
    if(filter){
        var item;
        if(filter.where)
            item = filter.where;
        else
            item = filter;
      
        // Build new filter
        for (var property in item) {
            if (item.hasOwnProperty(property)) {
                if(newfilter.length>0) newfilter +=" AND "
                    else newfilter = "where "
                if(item[property].inq){
                    newfilter += 'n.' + property + ' IN ' + JSON.stringify(item[property].inq);
                }else{
                    newfilter += 'n.' + property + ' = ' + JSON.stringify(item[property]);
                }
            }
        }
    }
 

    var query = ('match (n:%node%) %filter% return n')
        .replace('%node%', model)
    query = query.replace('%filter%', newfilter);
 
    // Passing filter in the query itself without parameters
    var params = {};

    this.db.query(query, params, function (err, results) {
        if (err) return cb(err);
        cb(null, results.map(function (r) {
            return r.n.data;
        }));
    });
}


neo4j.find = function (model, id, cb) {
    this.db.getNodeById(id, function (err, node) {
        if (err) return cb(err);
        cb(null, node.data);
    });
}

neo4j.exists = function (model, id, cb) {
    console.log('exists...')
    cb();
}

neo4j.destroy = function (model, id, cb) {
    console.log('in delete');
    var query = 'match (n:%node%) where (n.id) = {id} delete n'
        .replace('%node%', model);
    var params = { id: Number(id) };
    this.db.query(query, params, cb);
}

 neo4j.destroyAll = function(model, where, cb) {
    console.log('in deleteAll');
    var query = 'match (n:%node%) where (n.id) = {id} delete n'
        .replace('%node%', model);
    var params = where;
    this.db.query(query, params, cb);
}

neo4j.count = function (model, cb, where) {
    console.log('count', where);
    cb();
};

neo4j.updateAttributes = function (model, id, data, cb) {
    console.log('updateAttributes', id, model);
    var payload = '';


    // Get existing model
    var searchquery = ('match (n:%node% {id:{id}})  return n')
        .replace('%node%', model)
 
    // Passing filter in the query itself without parameters
    var searchparams = {id: id};
    var self = this;

    this.db.query(searchquery, searchparams, function (err, results) {
        if(err) return cb(err);

        results.map(function (r) {
            // Compare data
            for (var property in data) {
                if (data.hasOwnProperty(property)) {
                    // Check new data vs what's provided
                    if(JSON.stringify(data[property]) != JSON.stringify(r.n.data[property])){
                        // Something changed here
                        if(payload.length>0) 
                            payload +=" , ";
                       
                        payload += 'n.' + property + ' = ' + JSON.stringify(data[property]);
                    }
                }
            }
            
            var query = ('match (n:%node% {id:"%id%"}) set ' + payload  + ' return n;')
            .replace('%node%', model)
            .replace('%id%', data.id);
            
            var params = {};
            self.db.query(query, params, function (err, results){
                if(err)
                 return cb(err);
                
                cb(null,results);
            });
        });
    });
};
