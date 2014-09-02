ES = ES || {};

var elastical = Npm.require('elastical');
var client;
var indexedCollections = [];

var checkClientConnection = function() {
  if (!client || !client.connected)
    console.error('Error connecting ES');
};

ES.connect = function(options) {
  console.log('Connecting ES')
  client = new elastical.Client(options.host, {protocol: options.protocol || 'http', port: options.port || 9200, auth: options.auth});
  client._request('/_cluster/health', Meteor.bindEnvironment(function(err) {
    if (err) {
      console.error('Error connecting to ES', err);
      return;
    }
    
    client.connected = true;
    
    console.log('Indexing collections');
    _.forEach(indexedCollections, function(index) {
      indexCollection(index);    
    })
  }));
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
  checkClientConnection();

  indexDef = getIndexedCollection(indexName);
  index = client.getIndex(indexName);

  // Set undefined and empty to null so Elasticsearch can detect those
  // fields and set a default value. Otherwise they won't be saved and 
  // search will fail.
  _.forEach(_.keys(doc), function(field) {
    if (!_.isNumber(doc[field]) && _.isEmpty(doc[field]))
      doc[field] = null;
  });

  // Change dateCreated format
  doc.dateCreated = new Date(doc.dateCreated).toJSON();

  // Get information related to doc on other collections
  _.forEach(indexDef.relations, function(rel) {
    if (rel.idField) {
      console.log('Inverse relation');
      var selector = {};
      selector[rel.idField] = doc._id;
      var fields = {};
      fields[rel.valuePath] = 1;
      var relItems = rel.collection.find(selector, fields).fetch();
      doc[rel.fieldName] = _.map(relItems, function(item) {
        var value = getValue(item, rel.valuePath.split('.'));
        return { value: [value]};
      });
    } else {
      console.log('Direct relation');
      var relItems = _.clone(doc[rel.fieldName]);
      doc[rel.fieldName] = [];
      _.forEach(relItems, function(relItem) {
        var item = rel.collection.findOne({_id: relItem});
        if (item)
          doc[rel.fieldName].push(item[rel.valuePath]);
      });  
    }
  });

  index.index(doc.hierId, doc, { id: doc._id }, Meteor.bindEnvironment(function (err, result) {
    if (!err) {
      console.log('Document indexed in ' + indexName);
      // Mark document
      var flag = {
        $set: {}
      };
      flag.$set['_es_' + indexName] = Date.now();
      collection.direct.update({_id: doc._id}, flag, {});
    }
    else
      console.log(err);
  }));
};

var getValue = function(doc, path) {
  var tmp = doc;
  _.forEach(path, function(field) {
    if (tmp)
      tmp = tmp[field];
  });

  return tmp;
};

ES.syncCollection = function(options) {
  var collection = options.collection; 
  var indexName = collection._name;

  // Index when client is connected
  indexedCollections.push({name: indexName, collection: collection, fields: options.fields, relations: options.relations});

  // Insert hook
  collection.after.insert(function(userId, doc) {
    indexDocument(indexName, collection, doc);
  });

  // Update hook
  collection.after.update(function(userId, doc, fieldNames, modifier, options) {
    indexDocument(indexName, collection, doc);
  });

  // Generate update hooks for all interted relations
  _.forEach(options.relations, function(rel) {
    if (rel.idField) {
      var relationIndexing = function(doc) {
        var idFieldSplitted = rel.idField.split('.');
        var root = idFieldSplitted[0];
        if (_.isArray(doc[root])) {
          var ids = _.map(doc[root], function(link) {
            var childPath = idFieldSplitted.slice(1, idFieldSplitted.length);
            return getValue(doc, childPath);
          });
        } else {
          var ids = [getValue(doc, idFieldSplitted)];
        }

        var items = collection.find({_id: {$in: ids}}).fetch();        
        _.forEach(items, function(item) {
          indexDocument(indexName, collection, item);
        })
      };
      rel.collection.after.update(function(userId, doc, fieldNames, modifier, options) {
        relationIndexing(doc);
      });
      rel.collection.after.insert(function(userId, doc) {
        relationIndexing(doc);
      });
    }
  }); 
};

// elastical.Client.prototype._request = _.wrap(elastical.Client.prototype._request, function(fn) {
//   console.log(arguments);
//   console.log(this);
//   fn.call(this, arguments[1], arguments[2], arguments[3]);
// });

Meteor.methods({
  'esSearch': function(index, query, filters, highlight) {
    checkClientConnection();

    query.bool.minimum_should_match = 1;

    if (filters.bool.must.length > 0) {
      query = {
        filtered: {
          query: query,
          filter: filters
        }
      };
      console.dir(query.filtered.filter.and)
    }

    var async = Meteor._wrapAsync(
      Meteor.bindEnvironment(function(cb) {
        client.search({query: query, highlight: highlight, type: Meteor.user().hierId, index: index}, function(err, result) {
          cb(err, result);
        })
      })
    );

    return async();
  },
});


