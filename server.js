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

  _.forEach(_.keys(doc), function(field) {
    if (_.isEmpty(doc[field]))
      doc[field] = null;
  });

  _.forEach(indexDef.relations, function(rel) {
    var relItems = _.clone(doc[rel.fieldName]);
    doc[rel.fieldName] = [];
    _.forEach(relItems, function(relItem) {
      console.log('fetching relation values for es')
      var item = rel.collection.findOne({_id: relItem});
      if (item)
        doc[rel.fieldName].push(item[rel.valuePath]);
    });
  });

  index.index(Meteor.user().hierId, doc, { id: doc._id }, Meteor.bindEnvironment(function (err, result) {
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
  indexedCollections.push({name: indexName, collection: collection, fields: options.fields, relations: options.relations});

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
        client.search({query: query, type: Meteor.user().hierId, index: index}, function(err, result) {
          cb(err, result);
        })
      })
    );

    return async();
  },
});


