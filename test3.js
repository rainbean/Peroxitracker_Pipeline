//var azure = require('azure');
var S = require('string');
var azure = require('azure-mgmt-compute');
var fs = require('fs');

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

g_queue.createMessage('plates', 'PBD GFP-Hoechst_LOPAC_2_1', function(err) {
  if (err) {
    console.log('Failed to create message: ' + err);
  }
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
