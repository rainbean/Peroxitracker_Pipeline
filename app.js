// global configuration
var g_storageAccount = 'bioimage';
var g_storageAcessKey = 't2cCFG4nKcwSp4NrnghpI9fnZZ3hR8YvEYshRocCAzXJ5u3dSEx+b5sA05URmKk1MOFwVwStHa+d1la6TMauxA==';
var g_tmpFolder = '/tmp/';
var g_runtimeFolder = '/AzureRuntime';
var g_container = 'images';
var g_subscriberID = '3f5b2a4b-ea60-4b1c-b36b-c85001f46ac8';
var g_serviceName = 'Peroxitracker';
var g_currenency = 2;

// global variables
var azure = require('azure');
var g_blob = azure.createBlobService(g_storageAccount, g_storageAcessKey);
var log4js = require('log4js');
log4js.replaceConsole();

/**
 * Preprocessing: enhance image's quality  
 */
function enhanceImage(plate, callback) {
  var fs = require('fs');
  var async = require('async');
  var spawn = require('child_process').spawn;
  //regular express to match 'A - 3(fld 1 wv DAPI - DAPI).tif', with result 'A' and '3'
  var pattern = new RegExp(/^(.+) - (.+)\(.+\).tif$/);
  
  console.log('<=== Step 1&2: Enhance Image Quality ===>');
  
  // list
  fs.readdir(g_tmpFolder + plate, function (err, files) {
    if (err) {
      console.error('Failed to list DAPI subfolder of plate: ' + plate);
      return callback(err);
    }
    
    async.eachLimit(files, g_currenency, function (file, callback) {
      var well = file.match(pattern);
      if (!well) {
        // not matched file name, ignore it;
        return callback(); // no argument imply silent failback to next async.each
      }

      var fork = spawn('java', 
          ['-jar', g_runtimeFolder + '/PeroxiTracker_Standalone/PeroJava.jar', // jar path
           g_tmpFolder + plate + '/' + file]); // input filepath
      console.log('>>> Processing: ' + file);
  
      // debug purpose
      fork.stdout.on('data', function (data) {
        console.debug(' ' + data);
      });
  
      // debug purpose
      fork.stderr.on('data', function (data) {
        console.debug('>> ' + data);
      });
  
      // handle exit code
      fork.on('close', function (code) {
        callback( code !== 0 ? code : null);
      });
    }, function(err) {
      // if any of the saves produced an error, err would equal that error
      return callback(err, plate);
    });
  });
}

/**
 * Content screening: count # of cells  
 */
function countCells(plate, callback) {
  var fs = require('fs');
  var async = require('async');
  var spawn = require('child_process').spawn;
  // regular express to match 'A - 3(fld 1 wv DAPI - DAPI).tif', with result 'A' and '3'
  var pattern = new RegExp(/^(.+) - (.+)\(.+\).tif$/);
  
  console.log('<=== Step 3: Content screening: count # of cells ===>');
  
  // list
  fs.readdir(g_tmpFolder + plate + '/DAPI', function (err, files) {
    if (err) {
      console.error('Failed to list DAPI subfolder of plate: ' + plate);
      return callback(err);
    }
    
    async.eachLimit(files, g_currenency, function (file, callback) {
      var well = file.match(pattern);
      if (!well) {
        // not matched file name, ignore it;
        console.warn('Invalid filename format, ignored: ' + file);
        return callback(); // no argument imply silent failback to next async.each
      }

      var fork = spawn(g_runtimeFolder + '/PeroxiTracker_Matlab/onewellCellCounting.exe', // program path
          [g_tmpFolder + plate + '/DAPI/' + file, // input file path
           g_tmpFolder + plate + '/Result/' + well[1] + '_' + well[2] + '_cell_obj_cords.txt']); // output file path
      console.log('>>> Processing: ' + file);
      
      // debug purpose
      fork.stdout.on('data', function (data) {
        console.debug('> ' + data);
      });

      // debug purpose
      fork.stderr.on('data', function (data) {
        console.debug('>> ' + data);
      });

      // handle exit code
      fork.on('close', function (code) {
        callback( code !== 0 ? code : null);
      });
    }, function(err) {
      // if any of the saves produced an error, err would equal that error
      return callback(err, plate);
    });
  });
}

/**
 * Content screening: calculate tophat of wells  
 */
function calcTophat(plate, callback) {
  var fs = require('fs');
  var async = require('async');
  var spawn = require('child_process').spawn;
  // regular express to match 'A - 3(fld 1 wv FITC - FITC).tif', with result 'A' and '3'
  var pattern = new RegExp(/^(.+) - (.+)\(.+\).tif$/);
  
  console.log('<=== Step 4: Content screening: calculate tophat of wells ===>');

  // list
  fs.readdir(g_tmpFolder + plate + '/FITC', function (err, files) {
    if (err) {
      console.error('Failed to list FITC subfolder of plate: ' + plate);
      return callback(err);
    }
    
    async.mapLimit(files, g_currenency, function (file, callback) {
      var well = file.match(pattern);
      if (!well) {
        // not matched file name, ignore it;
        console.log('Invalid filename format, ignored: ' + file);
        return callback(); // no argument imply silent success
      }

      var fork = spawn(g_runtimeFolder + '/PeroxiTracker_Matlab/onewellTophat.exe', // program path
          [g_tmpFolder + plate + '/FITC/' + file, // input file path
           g_tmpFolder + plate + '/Tophat/' + well[1] + '_' + well[2] + '_tophat.mat']); // output tophat file path
      console.log('>>> Processing: ' + file);
      
      // debug purpose
      fork.stdout.on('data', function (data) {
        console.debug('> ' + data);
      });

      // debug purpose
      fork.stderr.on('data', function (data) {
        console.debug('>> ' + data);
      });

      // handle exit code
      fork.on('close', function (code) {
        callback(null, code);
      });
    }, function(err, results) {
      // if any of the saves produced an error, err would equal that error
      var found = false;
      for (var i=0; i<results.length; i+=1) {
        found |= results[i];
      }
      callback(err, plate, found);
    });
  });
}

/**
 * Content screening: calculate histogram 
 */
function calcHistorgram(plate, found, callback) {
  var fs = require('fs');
  var async = require('async');
  var spawn = require('child_process').spawn;
  
  console.log('<=== Step 5: Content screening: calculate histogram ===>');

  var fork = spawn(g_runtimeFolder + '/PeroxiTracker_Matlab/onePlateHistCalc.exe', // program path
      [g_tmpFolder + plate + '/Tophat', found]); // input path & union result of step 4 
  // implicit output is Tophat/netHist.mat
  console.log('>>> Processing with found: ' + found);

  // debug purpose
  fork.stdout.on('data', function (data) {
    console.debug('> ' + data);
  });

  // debug purpose
  fork.stderr.on('data', function (data) {
    console.debug('>> ' + data);
  });

  // handle exit code
  fork.on('close', function (code) {
    callback(code !== 0 ? code : null, plate);
  });
}

/**
 * Content screening: calculate feature set
 */
function calcFeature(plate, callback) {
  var fs = require('fs');
  var async = require('async');
  var spawn = require('child_process').spawn;
  // regular express to match 'A_3_tophat.mat', with result 'A' and '3'
  var pattern = new RegExp(/^(.+)_(.+)_tophat.mat$/);

  console.log('<=== Step 6: Content screening: calculate feature set ===>');
  
  // list
  fs.readdir(g_tmpFolder + plate + '/Tophat', function (err, files) {
    if (err) {
      console.error('Failed to list Tophat subfolder of plate: ' + plate);
      return callback(err);
    }
    
    async.eachLimit(files, g_currenency, function (file, callback) {
      var well = file.match(pattern);
      if (!well) {
        // not matched file name, ignore it;
        console.warn('Invalid filename format, ignored: ' + file);
        return callback(); // no argument imply silent failback to next async.each
      }

      var fork = spawn(g_runtimeFolder + '/PeroxiTracker_Matlab/onewellFeatGen.exe', // program path
          [g_tmpFolder + plate + '/Tophat/' + file, // input file path
           g_tmpFolder + plate + '/Result/' + well[1] + '_' + well[2] + '_feature.txt', // output file path
           g_tmpFolder + plate + '/Tophat/netHist.mat']); // input file from implicit output of step 5 
      console.log('>>> Processing: ' + file);
      
      // debug purpose
      fork.stdout.on('data', function (data) {
        console.debug('> ' + data);
      });

      // debug purpose
      fork.stderr.on('data', function (data) {
        console.debug('>> ' + data);
      });

      // handle exit code
      fork.on('close', function (code) {
        callback( code !== 0 ? code : null);
      });
    }, function(err) {
      // if any of the saves produced an error, err would equal that error
      return callback(err, plate);
    });
  });
}


function getFileLines(path, callback) {
  var fs = require('fs');
  var lines = 0;
  fs.createReadStream(path).on('data', function(chunk) {
    for (var i=0; i < chunk.length; i+=1) {
      if (chunk[i] === 10) {
        lines++; // line feed
      }
    }
  }).on('end', function() {
    callback(lines);
  });
}


/**
 * Content screening: Consolidate CSV file
 */
function genCSV(plate, callback) {
  var fs = require('fs');
  var S = require('string');
  var async = require('async');
  //regular express to match 'A_1_feature.txt', with result 'A' and '1'
  var pattern = new RegExp(/^(.+)_(.+)_feature.txt$/);
  
  console.log('<=== Step 7: Consolidate CSV file ===>');
  
  // unlink file first
  fs.unlink(g_tmpFolder + plate + '.csv', function(err) {});
  
  // list
  fs.readdir(g_tmpFolder + plate + '/Result', function (err, files) {
    if (err) {
      console.error('Failed to list Result subfolder of plate: ' + plate);
      return callback(err);
    }
    
    // iterate file list
    async.each(files, function (file, callback) {
      var well = file.match(pattern);
      if (!well) {
        // not matched file name, ignore it;
        return callback();
      }

      // well's data in array
      var arr = [];
      arr.push(well[1]); // row: A-AF
      arr.push(well[2]); // column 1-48

      getFileLines(g_tmpFolder + plate + '/Result/' + well[1] + '_' + well[2] + '_cell_obj_cords.txt', function(lines) {
        arr.push(lines); // # of cells
        
        fs.readFile(g_tmpFolder + plate + '/Result/' + file, function(err, data) {
          if (err) {
            console.error('Failed to read feature: ' + file);
            return callback(err);
          }
          
          var features = S(data).trim().split('  ');
          arr = arr.concat(features);
          
          // convert to CSV format
          fs.appendFileSync(g_tmpFolder + plate + '.csv', S(arr).toCSV().s + '\n');
          
          // iterate next file
          callback();
        }); // end of fs.readFile
      }); // end of getFileLines
    }, function(err) {
      // if any of the saves produced an error, err would equal that error
      return callback(err, plate);
    }); // end of async.each
  }); // end of file.readdir
}

/**
 * Archive result back to online storage, and mark file processed
 */
function feedbackBlob(plate, callback) {
  g_blob.putBlockBlobFromFile(g_container, plate + '.csv', g_tmpFolder + plate + '.csv', function (err, blob) {
    if (err) {
      console.error('Failed to upload CSV file');
      return callback(err);
    }
    
    console.log('CSV result uploaded');
    callback();
  });
}

/**
 * After images of a plate has been downloaded to local disk, process these images 
 */
function processPlate(plate, callback) {
  var async = require('async');

  async.waterfall([
      function(callback) {
        // step 1: enhance FITC files and save to FITC folder
        // step 2: enhance DAPI files and save to DAPI folder
        enhanceImage(plate, callback);
      },
      countCells, // step 3: count # of cells
      calcTophat, // step 4: calculate tophat of wells
      calcHistorgram, // step 5: calculate histogram
      calcFeature, // step 6: calculate feature set
      genCSV, // step 7: consolidate CSV file
      feedbackBlob // upload result back to Azure blob
    ],
    function(err, results) {
      if (err) {
        console.error('Failed to complete plate, error: ' + err);
        return callback(err);
      }
      console.log('>>>>>>> Plate process complete: ' + plate);
      return callback();
    });
}

/**
 * Fetch a plate
 */
function fetchPlate(plate, callback) {
  var S = require('string');
  var fs = require('fs');
  var async = require('async');

  console.log('Process plate: ' + plate);
  g_blob.listBlobs(g_container, {prefix: plate /*, include: 'metadata'*/}, function(err, blobs) {
    if (err) {
      console.error('Failed to access Azure Blob: ' + err);
      return callback(err);
    }
    
    if (!blobs || blobs.length === 0) {
      console.warn('Failed to fetch plate images: ' + plate);
      return callback('no file');
    }
    
    // create temp folder
    if (!fs.existsSync(g_tmpFolder + plate)) {
      fs.mkdirSync(g_tmpFolder + plate);
      fs.mkdirSync(g_tmpFolder + plate + '/DAPI');
      fs.mkdirSync(g_tmpFolder + plate + '/FITC');
      fs.mkdirSync(g_tmpFolder + plate + '/Tophat');
      fs.mkdirSync(g_tmpFolder + plate + '/Result');
    }

    // Async download file in parallel
    async.mapLimit(blobs, g_currenency, function (blob, callback) {
      //console.log(blob.name);
      //console.log(blob.metadata);
      //console.log(blob.properties);
      
      if (S(blob.name).contains('enhanced') || !S(blob.name).endsWith('.tif')) {
        return callback(); // ignore this file silently
      }
      
      fs.stat(g_tmpFolder + blob.name, function(err, stats) {
        if (err || stats.size != blob.properties['content-length']) {
          // file not ready, fetch file to local temporary storage
          console.log('Download ' + blob.name);
          g_blob.getBlobToFile(g_container, blob.name, g_tmpFolder + blob.name, callback);
        } else {
          return callback(); // skip file download
        }
      });
      
    }, function (err, results) {
      if (err) {
        console.error('Failed to fetch plate image: ' + err);
        return callback(err);
      }
      
      processPlate(plate, callback);
    });
  });
}

function shutdown() {
  var S = require('string');
  var fs = require('fs');
  var mc = require('azure-mgmt-compute');
  var os = require("os");
  
  var pem = S(fs.readFileSync(g_runtimeFolder + '/Peroxitracker_Pipeline/azure-hsu.pem')).s;
  var credential = mc.createCertificateCloudCredentials({
    subscriptionId: g_subscriberID,
    pem: pem
  });
  
  var client = mc.createComputeManagementClient(credential);
  
  // get service detail, which we need deployment name
  client.hostedServices.getDetailed(g_serviceName, function(err, data) {
    if (err) {
      console.log(err);
      process.exit(1);
    }
    
    //shutdown this compute node
    client.virtualMachines.shutdown(g_serviceName, data.deployments[0].name,
        os.hostname(), {postShutdownAction: 'StoppedDeallocated'}, function (err) {
      if (err) {
        console.error(err);
      }
      process.exit(0);
    });
  });
}

/**
 *  main function
 */
function main(callback) {
  var fs = require('fs');
  var version = 2;
  var async = require('async');
  
  // create temp folder
  if (!fs.existsSync(g_tmpFolder)) {
    fs.mkdirSync(g_tmpFolder);
  }
  
  if (version === 1) {
    /****************************
     * V1: decide plates to process by a plate.json file, not scale-able
     ****************************/
    console.log('Fetching plate.json ...');
    g_blob.getBlobToFile(g_container, 'plate.json', g_tmpFolder + 'plate.json', function (err, blob) {
      if (err) {
        console.error('No plate list to be processed, or error occured to fetch it: ' + err);
        return callback(err);
      }
      console.log('plate.json fetched');
      
      var plates = require(g_tmpFolder + 'plate.json');
      if (!plates || !Array.isArray(plates)) {
        console.error('failed to load plate.json');
        return callback('no data');
      }
      
      async.eachSeries(plates, fetchPlate, callback);
    });
  } else if (version === 2) {
    /****************************
     * V2: decide plates to process by Azure Blob Queue, support multiple instance 
     ****************************/
    console.log('Fetching plate message queue ...');
    var mq = azure.createQueueService(g_storageAccount, g_storageAcessKey);
    var hasData = true;

    mq.createQueueIfNotExists('plates', function (err) {
      if (err) {
        console.error('Failed to create queue: ' + err);
        return callback(err);
      }
    });

    async.whilst(
      function () {
        return hasData;
      },
      function(callback) {
        mq.getMessages('plates', function(err, message) {
          if (err) {
            hasData = false;
            console.error('Failed to fetch message queue: ' + err);
            return callback(err);
          }
          //console.log(serverMessages);
          if (!message || !message.length) {
            // no message to process
            hasData = false;
            console.warn('No plate to process');
            return callback('no data');
          }
          fetchPlate(message[0].messagetext, function(err) {
            if (!err) {
              // when a plate is processed competely, remove it from MQ
              mq.deleteMessage('plates', message[0].messageid, message[0].popreceipt, function(err) {
                if (err) {
                  console.log('Failed to remove message: ' + err);
                }
              });
            }
            return callback(err);
          });
        });
      }, callback
    ); // end of async.whilst
  }
}

main(function (err) {
  // shutdown this compute node after 5 minutes
  setTimeout(shutdown, 5*60*1000);
});
