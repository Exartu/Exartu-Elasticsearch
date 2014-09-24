Package.describe({
  summary: "ES"
});

var both = ["client", "server"];

Npm.depends({
  "elastical": "0.0.13",
});

Package.onUse(function(api){
	api.use([
    	"matb33:collection-hooks",
    	"underscore",
    ], "server");

  api.addFiles("server.js", "server");
  api.addFiles("client.js", "client");

  api.export("ES", both);
});