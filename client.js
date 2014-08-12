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
      var regexp = {};
      regexp[field] = searchString;
      q.push({regexp: regexp});
	  });
  	Meteor.call('esSearch', options.collection._name, query, function(err, result) {
  		if (!err) {
  			console.log(result);
  		}

  		cb && cb.call(err, result);
  	});
  };
}