ES = ES || {};

ES.syncCollection = function(options) {
  var collection = options.collection;

  collection.esSearch = function(searchString, cb) {
  	var splitedSearchString = searchString.split(" ");
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

      _.forEach(splitedSearchString, function(tokenSearch) {
        var regexp = {};
        regexp[field] = tokenSearch;
        q.push({regexp: regexp});
      })
	  });

    console.log(query)
  	Meteor.call('esSearch', collection._name, query, function(err, result) {
  		if (!err) {
  			console.log(result);
  		}

  		cb && cb.call({}, err, result);
  	});
  };
}