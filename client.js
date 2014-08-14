ES = ES || {};

ES.syncCollection = function(options) {
  var collection = options.collection;
  collection.esSearch = function(searchString, cb) {
  	var query = {
  		bool: {
        should: [],
      }
  	};
    var q = query.bool.should;
    q.regexp = {};
	  _.forEach(options.fields, function(field) {
      if (_.isObject(field)) {
        field = field.name;
      }
      
      var regexp = {};
      regexp[field] = searchString;
      q.push({regexp: regexp});
	  });

    console.log(query)
  	Meteor.call('esSearch', collection._name, query, function(err, result) {
  		if (!err) {
  			console.log(result);
  		}

  		cb && cb.call(err, result);
  	});
  };

  collection.esSuggest = function(text, cb) {
    Meteor.call('esSuggest', collection._name, text, function(err, result) {
      if (!err) {
        console.log(result);
      }

      cb && cb.call(err, result);
    });
  };
}