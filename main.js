#!/usr/bin/env node

var cli = require("cli"),
    _   = require("underscore"),
    request = require("request");

var commands = {
  list: {
    desc: "List all running replications and their statuses.",
    usage: "couch-parrot [Options] list",
    cmdOptions: [

    ],
    fn: function() {
      var opts = {
        uri: 'http://' + cliOptions.host + ":" + cliOptions.port + "/_replicator/_all_docs?include_docs=true",
        method: "get"
      };
      if (cliOptions.username) {
        opts.username = cliOptions.username;
        opts.password = cliOptions.password;
      }
      send(opts, function(err, res, body) {
        if (err || res.statusCode !== 200) {
          cli.error("Error getting replications: " + res.statusCode + "; " + err);
        } else {
          var result = JSON.parse(body);
          if (result.total_rows <= 0) {
            cli.ok("No replications found.");
          } else {
            cli.ok((result.total_rows - 1) + " replications found.\n\nID                  Replication ID      State          ");
            var rows = _.pluck(_.reject(result.rows, function(doc) { return doc.id.indexOf('_design') === 0; }), 'doc');
            var text;
            _.each(rows, function(row) {
              text = row._id + " | ";
              if (row._replication_id) {
                text += row._replication_id + " | ";
                text += row._replication_state;
              }
              cli.ok(text);
            });
          }
        }
      });
    }
  },

  add: {
    desc: "Add a replication document.",
    usage: "couch-parrot add [options]",
    cmdOptions: [

    ],
    fn: function() {

      if (! cliOptions.source || ! cliOptions.target || ! cliOptions.id) {
        cli.fatal('id, source and target options are required.');
      }

      var opts = {
        uri: 'http://' + cliOptions.host + ":" + cliOptions.port + "/_replicator/" + cliOptions.id,
        method: "put"
      };
      if (cliOptions.username) {
        opts.username = cliOptions.username;
        opts.password = cliOptions.password;
      }
      
      opts.data = {
        "_id": cliOptions.id,
        source: cliOptions.source,
        target: cliOptions.target,
        continuous: cliOptions.continuous,
        create_target: cliOptions.createTarget,
        user_ctx: {
          name: "admin",
          roles: ["_admin"]
        }
      };
      send(opts, function(err, res, body) {
        if (err || res.statusCode !== 201) {
          cli.error("Error adding replication: status " + res.statusCode + "; " + (err || body.reason));
        } else {
          cli.ok("Replication " + cliOptions.id + " added: " + JSON.stringify(body));
        }
      });
    }
  },

  remove: { 
    desc: "Removes the specified replication document",
    usage: "couch-parrot [options] remove",
    cmdOptions: [

    ],
    fn: function() {
      if (! cliOptions.id) {
        cli.fatal('The id option is required');
      }

      var getOpts = {
        uri: 'http://' + cliOptions.host + ":" + cliOptions.port + "/_replicator/" + cliOptions.id,
        method: "get"
      };

      var delOpts = {
        uri: 'http://' + cliOptions.host + ":" + cliOptions.port + "/_replicator/" + cliOptions.id,
        method: "delete"
      };
      if (cliOptions.username) {
        delOpts.username = getOpts.username = cliOptions.username;
        delOpts.password = getOpts.password = cliOptions.password;
      }

      send(getOpts, function(getErr, getRes, getBody) {
        if (getErr || getRes.statusCode !== 200) {
          cli.fatal('Error retrieving replication: status ' + getRes.statusCode + "; " + (getErr || getBody.reason));
        }
        var doc = JSON.parse(getBody);
        delOpts.uri += "?rev=" + doc._rev;

        send(delOpts, function(err, res, body) {
          if (err || res.statusCode !== 200) {
            cli.error("Error removing replication: status " + res.statusCode + "; " + (err || body.reason));
          } else {
            cli.ok("Replication " + cliOptions.id + " deleted.  Final state: " + JSON.stringify(doc));
          }
        });
      });
    }
  },

  status: {
    desc: "Get the status of a single replication task",
    usage: "couch-parrot status [options]",
    cmdOptions: [

    ],
    fn: function() {
      if (!cliOptions.id) {
        cli.fatal('The id option is required.');
      }
      var opts = {
        uri: 'http://' + cliOptions.host + ":" + cliOptions.port + "/_replicator/" + cliOptions.id,
        method: "get"
      };
      if (cliOptions.username) {
        opts.username = cliOptions.username;
        opts.password = cliOptions.password;
      }
      send(opts, function(err, res, body) {
        if (err || res.statusCode !== 200) {
          cli.error("Error getting replication: " + res.statusCode + "; " + err);
        } else {
          var result = JSON.parse(body);
          cli.ok('Replication ' + result._id + ' has status ' + result._replication_state + " as of " + result._replication_state_time);
        }
      });
    }
  }
};

var cliOptions = cli.parse({
  host: ['h', 'The backup server host to connect to', 'string', 'localhost'],
  port: ['P', 'The backup server port to use', 'string', '5984'],
  source: ['s', 'The source argument for replications (add command)', 'string', null],
  target: ['t', 'The target argument for replications (add command)', 'string', null],
  id: ['i', 'Replication id to work with (remove and status commands)', 'string', null],
  username: ['u', 'Username to connect to host with', 'string', null],
  password: ['p', 'Password to connect to host with', 'string', null],
  continuous: ['c', 'Continuous replication or not (add command)', 'boolean', false],
  createTarget: ['C', 'Create target database (add command)', 'boolean', false]
}, 
['list', 'add', 'remove', 'status']);

commands[resolveCmd(cli.command)].fn.call();

function resolveCmd(cmd) {
  return cmd.replace(/-([a-z])/, function(m) { return m[1].toUpperCase(); });
}

/**
 *  Available Options: 
 *  uri, method, data, username, password
 */
function send(options, cb) {

  var reqOpts = {
    uri: options.uri,
    method: options.method.toUpperCase(),
    headers: {}
  };

  if (options.data) {
    reqOpts.json = options.data;
  }
  if (options.username) {
    reqOpts.headers.Authorization = "Basic " + new Buffer(options.username + ":" + options.password).toString("base64");
  }

  request(reqOpts, cb);
}