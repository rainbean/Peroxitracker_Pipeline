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

var client = azure.createComputeManagementClient(credential);


// get service detail, which we need deployment name
client.hostedServices.getDetailed('Peroxitracker', function(err, data) {
  if (err) {
    console.log(err);
    process.exit(1);
  }
  
  //shutdown this compute node
  client.virtualMachines.shutdown('Peroxitracker', data.deployments[0].name,
      'node1', {postShutdownAction: 'StoppedDeallocated'}, function (err) {
    if (err) {
      console.error(err);
    }
    process.exit(0);
  });
});

/*
vm.virtualMachines.start('Peroxitracker', 'production', 'node1', function (err) {
  if (err) {
    console.error(err);
  }
});
*/

/*
vm.virtualMachines.shutdown('Peroxitracker', 'TestMatlab', 'node1', {PostShutdownAction: 'StoppedDeallocated'}, function (err) {
  if (err) {
    console.error(err);
  }
});
*/