//var azure = require('azure');
var S = require('string');
var fs = require('fs');
var async = require('async');

var g_storageAccount = 'bioimage';
var g_storageAcessKey = 't2cCFG4nKcwSp4NrnghpI9fnZZ3hR8YvEYshRocCAzXJ5u3dSEx+b5sA05URmKk1MOFwVwStHa+d1la6TMauxA==';
var azure = require('azure');
var g_queue = azure.createQueueService(g_storageAccount, g_storageAcessKey);

g_queue.createQueueIfNotExists('plates', function (err) {
  if (err) {
    console.log('Failed to create queue: ' + err);
  }
});

/*
g_queue.clearMessages('plates', function(err) {
  if (err) {
    console.log('Failed to create message: ' + err);
  }
});
*/

plates = [
'H1098495', 
'H1098497', 
'H1098499', 
'H1098501', 
'H1098503', 
'H1098505', 
'H1098507', 
'H1098509', 
'H1098511', 
'H1098513', 
'H1098515'
];

async.eachSeries(plates, function (plate, callback) {
  g_queue.createMessage('plates', plate, function(err) {
    if (err) {
      console.log('Failed to create message: ' + plate + ', error: ' + err);
    }
	callback();
  });
});

/*
g_queue.getMessages('plates', function(err, serverMessages) {
  if (err) {
    console.log('Failed to create message: ' + err);
    return;
  }
  console.log(serverMessages);
});
*/
