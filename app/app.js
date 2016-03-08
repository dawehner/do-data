var elasticsearch = require('elasticsearch');
var request = require('request');

var client = new elasticsearch.Client({
  host: 'localhost:9200'
  // log: 'trace'
});

function fetchDoComments(nodeId, page) {
  request('https://www.drupal.org/api-d7/comment.json?node=' + nodeId + '&page=' + page, function (error, response, body) {
    if (!error && response.statusCode == 200) {
      body = JSON.parse(body);
      var length = body['list'].length;
      var entry;
      var i;
      for (i = 0; i < length; i++) {
        entry = body.list[i];
        createComment(client, entry);
      }
      fetchDoComments(nodeId, page+1);
    }
  });
}

fetchIssues(client, 0)

function createIssue(client, entry) {
  entry['comments'] = [];
  entry['flag_project_issue_follow_user'] = [];

  client.create({
    index: "do",
    type: "do-issue",
    id: entry['nid'],
    body: entry
  }, function (error, response) {
  });
}

function createComment(client, entry) {
  client.create({
    index: "do",
  type: "do-comment",
  id: entry['cid'],
  body: entry
  }, function (error, response) {
  });
}

function fetchIssues(client, page) {
  var url = "https://www.drupal.org/api-d7/node.json?limit=50&type=project_issue&field_project=3060&page=" + page;
  console.log(url);

  request(url, function (error, response, body) {
    if (!error && response.statusCode == 200) {
      body = JSON.parse(body);
      var length = body['list'].length;
      var entry;
      var i;
      var j;
      var comments_length;
      for (i = 0; i < length; i++) {
        entry = body.list[i];

        comments_length = entry['comments'].length;
        for (j = 0; j < comments_length; j++) {
          createComment(client, entry['comments'][j]);
        }

        createIssue(client, entry);
      }
      fetchIssues(client, page+1);
    }
    else {
      console.log(error);
    }
  });
}

function countEntries(client, callable) {
  client.count({
    index: 'do'
  }, callable);
}

function pingEs(client) {
  client.ping({
    // ping usually has a 3000ms timeout
    requestTimeout: Infinity,

    // undocumented params are appended to the query string
    hello: "elasticsearch!"
  }, function (error) {
    if (error) {
      console.trace('elasticsearch cluster is down!');
    } else {
      console.log('All is well');
    }
  });
}
