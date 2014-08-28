ES = ES || {};

ES.syncCollection = function(options) {
  var collection = options.collection;

  collection.esSearch = function(searchString, cb) {
  	var splitedSearchString = searchString.toLowerCase().trim().split(" ");
    var query = {
  		bool: {
        should: [],
      }
  	};
    var q = query.bool.should;

    var highlight = {
      "pre_tags" : ["<strong>"],
      "post_tags" : ["</strong>"],
      fields: {}
    };

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

      // Set highlight option for all fields defined
      highlight.fields[field] = {};
	  });

    console.log(query)
  	Meteor.call('esSearch', collection._name, query, highlight, function(err, result) {
  		if (!err) {
  			console.log(result);
  		}

      // Renaming highlight result
      _.forEach(result.hits, function(hit) {
        _.forEach(hit.highlight, function(value, propertyName) {
          var field = _.findWhere(options.fields, {name: propertyName});
          if (field && field.label) {
            hit.highlight[field.label] = hit.highlight[propertyName];
            delete hit.highlight[propertyName];
          }
        })
      });

  		cb && cb.call({}, err, result);
  	});
  };
}