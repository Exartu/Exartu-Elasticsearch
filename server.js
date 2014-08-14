ES = ES || {};

var elastical = Npm.require('elastical');
var client;
var indexedCollections = [];

var checkClientConnection = function() {
  if (!client)
    throw new Meteor.Error(500, 'Error connecting ES');
};

ES.connect = function(options) {
  console.log('Connecting ES')
  client = new elastical.Client(options.host, {protocol: options.protocol, port: options.port, auth: options.auth});

  checkClientConnection();

  console.log('Indexing collections');
  _.forEach(indexedCollections, function(index) {
    indexCollection(index);    
  })
};

var initialSync = function(collection, indexName) {
  // Initial sync. Index all document not indexed
  var documents = collection.find().fetch();
  // Generate bulk's operation
  var operations = [];
  _.forEach(documents, function(document) {
    if (document['_es_' + indexName])
      return; // Already indexed
    var op = {
      index: indexName,
      type: document.hierId,
      id: document._id,
      data: document
    };
    operations.push({index: op});
  });

  if (!operations || operations.length == 0)
    return;

  client.bulk(operations, Meteor.bindEnvironment(function(err, result) {
    if (!err) {
      var documentIds = _.map(operations, function(op) {
        return op.index.id;
      });
      var flag = {
        $set: {}
      };
      flag.$set['_es_' + indexName] = Date.now();
      collection.update({_id: {$in: documentIds}}, flag, {multi: true});
    } else
      console.log(err);  
  }));
};

var indexCollection = function(index) {
  console.log('Index name: ' + index.name)
  client.indexExists(index.name, Meteor.bindEnvironment(function(err, result) {
    if (!err) {
      if (result) {
        console.log('Loading ' + index.name + ' index')
        initialSync(index.collection, index.name);
      }
      else {
        console.log('Creating ' + index.name + ' index') 
        client.createIndex(index.name, function(err, result) {
          if (!err) {
            console.log("Index created ", result);

            // Fields mapping 
            var properties = {};

            console.log(index.fields)
            _.forEach(index.fields, function(field) {
              if (_.isString(field)) {
                properties[field] = {
                  type: 'string',
                  null_value: 'na',
                }
              } else if (field && field.sugguster) {
                properties[field.name] = {
                  type: 'completion',
                  index_analyzer: "simple",
                  search_analyzer: "simple",
                  payloads: true
                }
              }
            });

            var jsonData = {};
            jsonData[index.name] = {
              properties: properties,
            };
            
            var ignore = "ignore_conflicts=true"; // Ignore conflict merging mapping with old documents

            client._request('/' + index.name + '/' + index.name + '/_mapping?' + ignore, {
                method: 'PUT',
                json: jsonData
              }, Meteor.bindEnvironment(function(err, result) {
                console.log(err, result);
              })
            );

            initialSync(index.collection, index.name);
          }
          else
            console.log(err);
        });
      }
    }
  }));
};

var getIndexedCollection = function(indexName) {
  return _.findWhere(indexedCollections, {name: indexName});
};

var indexDocument = function(indexName, collection, doc) {
  indexDef = getIndexedCollection(indexName);
  index = client.getIndex(indexName);

  // // Proccess suggesters fields
  // _.forEach(_.where(indexDef.fields, {sugguster: true}), function(fieldDef) {
  //   var value = doc[fieldDef.name];
  //   doc[fieldDef.name] = {
  //     input: value,
  //     payload: {_id: doc._id}
  //   }
  //   console.log(doc[fieldDef.name]);
  // });

  _.forEach(_.keys(doc), function(field) {
    if (_.isEmpty(doc[field]))
      doc[field] = null;
  });

  index.index(indexName, doc, { id: doc._id }, Meteor.bindEnvironment(function (err, result) {
    if (!err) {
      console.log('Document indexed in ' + indexName);
      // Mark document
      var flag = {
        $set: {}
      };
      flag.$set['_es_' + indexName] = Date.now();
      collection.direct.update({_id: doc._id}, flag);
    }
    else
      console.log(err);
  }));
};

ES.syncCollection = function(options) {
  var collection = options.collection; 
  var indexName = collection._name;

  // Index when client is connected
  indexedCollections.push({name: indexName, collection: collection, fields: options.fields});

  // Insert hook
  collection.after.insert(function(userId, doc) {
    checkClientConnection();
    indexDocument(indexName, collection, doc);
  });

  // Update hook
  collection.after.update(function(userId, doc, fieldNames, modifier, options) {
    checkClientConnection();
    indexDocument(indexName, collection, doc);
  });
};

Meteor.methods({
  'esSearch': function(index, query) {
    checkClientConnection();

    query.bool.minimum_should_match = 1;

    var async = Meteor._wrapAsync(
      Meteor.bindEnvironment(function(cb) {
        client.search({query: query, type: index/*Meteor.user().hierId */, index: index}, function(err, result) {
          cb(err, result);
        })
      })
    );

    return async();
  },
  'esSuggest': function(index, text) {
    var async = Meteor._wrapAsync(
      Meteor.bindEnvironment(function(cb) {
        // Get suggester fields
        client._request('/'+ index + '/_suggest', {
            method: 'POST',
            json: {
              // suggest: {
                notes : {
                  text : text,
                  completion : {
                    field: 'content',
                    "context": {
                      "_type" : "notes",
                      "additional_name": null
                    }                    
                  }
                // }
              }
            } 
          }, function(err, result) {
            cb(err, result);
          }
        );
      })
    );
    return async();
  }
});


