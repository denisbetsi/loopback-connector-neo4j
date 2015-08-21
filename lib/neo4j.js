'use strict';

var GraphDatabase = require('neo4j').GraphDatabase;

exports.initialize = function (dataSource, cb) {
    // console.log(dataSource);
    var settings = dataSource.settings;
    var db = new GraphDatabase(settings.neo4j_url);
    dataSource.connector = new Neo4j(db);
    dataSource.db = db;
    cb && cb();
    this.app = dataSource 
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
    var offset;
    var limit;
   
    var keyToSymbol = function (key) {
        return key + ':{filter}.' + key;
    }
   
    // var relations = model.settings.relations;
    console.log('Relations : ' + JSON.stringify(model));
    console.log('Filter ' + JSON.stringify(filter));

    // Build filter which supports IN query
    var newfilter = "";
    var subquery = "";
    var endquery = " return distinct(n) as n"
    var startquery = "match (n:%node%) %filter% ";
    if(filter){
        var item;
        if(filter.where)
            item = filter.where;
        else
            item = filter;
      
        // Build new filter
        var subModel;
        for (var property in item) {
            if (item.hasOwnProperty(property)) {

                // Exclude offset & limit
                if(property.toUpperCase()=='OFFSET'){
                    offset = item[property];
                }else if(property.toUpperCase()=='LIMIT'){
                    limit = item[property];
                }else{
                    // Check if this is a subquery or not
                    if(property.indexOf("Id", property.length - 2) !== -1){
                        // This is a sub query
                        subModel= property.replace("Id","");
                        // Check if reverse is present
                        // console.log('What is item : ' + JSON.stringify(item) + ' AND ' + JSON.stringify(item[property]) + ' SUPER AND ' + item[property].reverse );
                        if(item[property].reverse){
                            // Reversed lookup
                            startquery = "match (n:" + subModel + ")  where " + getInQuery("n.id",(item[property].inq ? item[property].inq : item[property]));
                            subquery += ' match (oo:' + model + ') %filter% with n,oo MATCH (n)-[r:HAS_' + model.toUpperCase() + ']->(oo) ';
                            // newfilter += ( newfilter.length > 0 ? " AND " : " where ") + " " + getInQuery("n.id",(item[property].inq ? item[property].inq : item[property]));
                            endquery = " return distinct(oo) as n";

                        }else{
                            // Proper way
                            subquery += ' MATCH (n)-[r:HAS_' + subModel.toUpperCase() + ']->(oo:' + subModel + ') where ' + getInQuery('oo.id', (item[property].inq ? item[property].inq : item[property]));
                            endquery = " return distinct(n) as n";
                        }
                    }else{
                        // Regular property
                        if(!item[property].reverse){
                            newfilter += (newfilter.length>0 ? " AND " : "where ") + getInQuery('oo.' + property,(item[property].inq ? item[property].inq : item[property])) ;
                        }
                    }                    
                }
                
            
            }
        }
        // If no submodels
        if(!subModel){
            // update filter
            newfilter = newfilter.split("oo.").join("n.");
        }
    }
 
    // console.log("start is : " + startquery);
    // console.log("subquery is : " + subquery);
    // console.log("endquery is : " + endquery);
    // console.log("new filter is : " + newfilter);
    
    var countSubQuery = " " + subquery.split("oo").join("ooo") + " with count(*) as total,n ";
    
    var query;

    if(subquery&&(offset||limit)){
        query = (startquery + countSubQuery + subquery + endquery + ",total ").replace('%node%', model);
    }else{
        query = (startquery + subquery + endquery).replace('%node%', model);  
    } 

    // var query = (startquery + countSubQuery + subquery + endquery + ",total ").replace('%node%', model);


    query = query.replace('%filter%', newfilter);
    // Check for skip & limit
    if(offset) query += ' SKIP ' + offset;
    if(limit) query += ' LIMIT ' + limit;
    console.log('QUERY : ' + query);
 
    // Passing filter in the query itself without parameters
    var params = {};

    this.db.query(query, params, function (err, results) {
        if (err) return cb(err);
        results=results.filter(function(r){return r.n.data.id});
        cb(null, results.map(function (r) {
            console.log("GOT DATA : ",r));
            return r.n.data;
        }));
    });
}

function getInQuery(propertyName,value){
    return 'has(' + propertyName + ') and any(bbKing in ' + propertyName + ' where bbKing in ' + (value.constructor === Array? JSON.stringify(value) : "[" + JSON.stringify(value) + "]") + ")";
        // return '';
}


neo4j.find = function (model, id, cb) {
    console.log('inside find');
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
    var query = 'match (n:%node%) where (n.id) = {id} with n optional match n-[r]-(o) delete r,n'
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

    for (var property in data) {
        if (data.hasOwnProperty(property)) {
            if(payload.length>0) 
                payload +=" , ";
            else
                payload = "{";
           
            payload += property + ' : ' + JSON.stringify(data[property]);
        }
    }
    payload += "}";


    var query = 'match (n:%node%)  where (n.id) = {id} set n=' + payload ;
    query = query.replace('%node%', model);
    console.log('Finaly query is : ' + query);

    var params = { id: data.id };
    this.db.query(query, params, cb);

   // cb();
};
