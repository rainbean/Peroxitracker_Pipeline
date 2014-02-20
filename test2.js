//var azure = require('azure');
var S = require('string');
var azure = require('azure-mgmt-compute');
var fs = require('fs');

//var credential = azure.createBasicAuthenticationCloudCredentials('azurehsu@cloudapp')

var pem = S(fs.readFileSync('azure-hsu.pem')).s;
//var pem = fs.readFileSync('azure-hsu.pem');
//console.log(pem);

var credential = azure.createCertificateCloudCredentials({
  subscriptionId: '3f5b2a4b-ea60-4b1c-b36b-c85001f46ac8',
  pem: pem
});

var vm = azure.createComputeManagementClient(credential);

vm.virtualMachines.start('Peroxitracker', 'TestMatlab', 'node1', function (err) {
  if (err) {
    console.error(err);
  }
});

/*
vm.virtualMachines.shutdown('Peroxitracker', 'TestMatlab', 'node1', {PostShutdownAction: 'StoppedDeallocated'}, function (err) {
  if (err) {
    console.error(err);
  }
});
*/