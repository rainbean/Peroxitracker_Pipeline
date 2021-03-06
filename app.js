// global configuration
var g_storageAccount = 'bioimage';
var g_storageAcessKey = 't2cCFG4nKcwSp4NrnghpI9fnZZ3hR8YvEYshRocCAzXJ5u3dSEx+b5sA05URmKk1MOFwVwStHa+d1la6TMauxA==';
var g_tmpFolder = 'd:/';
var g_runtimeFolder = '/AzureRuntime';
var g_container = 'images';
var g_subscriberID = '3f5b2a4b-ea60-4b1c-b36b-c85001f46ac8';
var g_serviceName = 'Peroxitracker';
var g_currenency = 1;

// global variables
var azure = require('azure');
var g_blob = azure.createBlobService(g_storageAccount, g_storageAcessKey);

/**
 * Configure Logger for debug purpose, save to console as well
 */
function initLogger() {
  var log4js = require('log4js');
  var fs = require('fs');

  try {
    fs.unlinkSync(g_tmpFolder + 'debug.log');
  } catch (e) {
    //doesn't really matter if it failed
  }

  log4js.configure({
    appenders: [
      { type: 'console' },
      { type: 'file', filename: g_tmpFolder + 'debug.log', category: 'logger' }
    ]
  });
  return log4js.getLogger('logger');
}
var logger = initLogger();


/**
 * Preprocessing: enhance image's quality  
 */
function enhanceImage(plate, callback) {
  var fs = require('fs');
  var async = require('async');
  var spawn = require('child_process').spawn;
  //regular express to match 'A - 3(fld 1 wv DAPI - DAPI).tif', with result 'A' and '3'
  var pattern = new RegExp(/^(.+) - (.+)\(.+\).tif$/);
  
  logger.info('<=== Step 1&2: Enhance Image Quality ===>');
  
  // list
  fs.readdir(g_tmpFolder + plate, function (err, files) {
    if (err) {
      logger.error('Failed to list DAPI subfolder of plate: ' + plate);
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
      logger.info('>>> Processing: ' + file);
  
      // debug purpose
      fork.stdout.on('data', function (data) {
        logger.debug(' ' + data);
      });
  
      // debug purpose
      fork.stderr.on('data', function (data) {
        logger.debug('>> ' + data);
      });
  
      // handle exit code
      fork.on('close', function (code) {
        if (code) {
          // log the problematic files for diagnostic. 
          logger.error('Failed to process [' + plate + '] [' + file + '], ignored it');
        }
        // callback( code !== 0 ? code : null);
        callback();
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
  
  logger.info('<=== Step 3: Content screening: count # of cells ===>');
  
  // list
  fs.readdir(g_tmpFolder + plate + '/DAPI', function (err, files) {
    if (err) {
      logger.error('Failed to list DAPI subfolder of plate: ' + plate);
      return callback(err);
    }
    
    async.eachLimit(files, g_currenency, function (file, callback) {
      var well = file.match(pattern);
      if (!well) {
        // not matched file name, ignore it;
        logger.warn('Invalid filename format, ignored: ' + file);
        return callback(); // no argument imply silent failback to next async.each
      }

      var fork = spawn(g_runtimeFolder + '/PeroxiTracker_Matlab/onewellCellCounting.exe', // program path
          [g_tmpFolder + plate + '/DAPI/' + file, // input file path
           g_tmpFolder + plate + '/Result/' + well[1] + '_' + well[2] + '_cell_obj_cords.txt']); // output file path
      logger.info('>>> Processing: ' + file);
      
      // debug purpose
      fork.stdout.on('data', function (data) {
        logger.debug('> ' + data);
      });

      // debug purpose
      fork.stderr.on('data', function (data) {
        logger.debug('>> ' + data);
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
  
  logger.info('<=== Step 4: Content screening: calculate tophat of wells ===>');

  // list
  fs.readdir(g_tmpFolder + plate + '/FITC', function (err, files) {
    if (err) {
      logger.error('Failed to list FITC subfolder of plate: ' + plate);
      return callback(err);
    }
    
    async.mapLimit(files, g_currenency, function (file, callback) {
      var well = file.match(pattern);
      if (!well) {
        // not matched file name, ignore it;
        logger.info('Invalid filename format, ignored: ' + file);
        return callback(); // no argument imply silent success
      }

      var fork = spawn(g_runtimeFolder + '/PeroxiTracker_Matlab/onewellTophat.exe', // program path
          [g_tmpFolder + plate + '/FITC/' + file, // input file path
           g_tmpFolder + plate + '/Tophat/' + well[1] + '_' + well[2] + '_tophat.mat']); // output tophat file path
      logger.info('>>> Processing: ' + file);
      
      // debug purpose
      fork.stdout.on('data', function (data) {
        logger.debug('> ' + data);
      });

      // debug purpose
      fork.stderr.on('data', function (data) {
        logger.debug('>> ' + data);
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
  
  logger.info('<=== Step 5: Content screening: calculate histogram ===>');

  var fork = spawn(g_runtimeFolder + '/PeroxiTracker_Matlab/onePlateHistCalc.exe', // program path
      [g_tmpFolder + plate + '/Tophat', found]); // input path & union result of step 4 
  // implicit output is Tophat/netHist.mat
  logger.info('>>> Processing with found: ' + found);

  // debug purpose
  fork.stdout.on('data', function (data) {
    logger.debug('> ' + data);
  });

  // debug purpose
  fork.stderr.on('data', function (data) {
    logger.debug('>> ' + data);
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

  logger.info('<=== Step 6: Content screening: calculate feature set ===>');
  
  // list
  fs.readdir(g_tmpFolder + plate + '/Tophat', function (err, files) {
    if (err) {
      logger.error('Failed to list Tophat subfolder of plate: ' + plate);
      return callback(err);
    }
    
    async.eachLimit(files, g_currenency, function (file, callback) {
      var well = file.match(pattern);
      if (!well) {
        // not matched file name, ignore it;
        logger.warn('Invalid filename format, ignored: ' + file);
        return callback(); // no argument imply silent failback to next async.each
      }

      var fork = spawn(g_runtimeFolder + '/PeroxiTracker_Matlab/onewellFeatGen.exe', // program path
          [g_tmpFolder + plate + '/Tophat/' + file, // input file path
           g_tmpFolder + plate + '/Result/' + well[1] + '_' + well[2] + '_feature.txt', // output file path
           g_tmpFolder + plate + '/Tophat/netHist.mat']); // input file from implicit output of step 5 
      logger.info('>>> Processing: ' + file);
      
      // debug purpose
      fork.stdout.on('data', function (data) {
        logger.debug('> ' + data);
      });

      // debug purpose
      fork.stderr.on('data', function (data) {
        logger.debug('>> ' + data);
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
  }).on('error', function() {
    logger.error('Failed to open cell file: ' + path);
    callback(0); // treat as no cell counted.
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
  
  logger.info('<=== Step 7: Consolidate CSV file ===>');
  
  // unlink file first
  fs.unlink(g_tmpFolder + plate + '.csv', function(err) {});
  
  // list
  fs.readdir(g_tmpFolder + plate + '/Result', function (err, files) {
    if (err) {
      logger.error('Failed to list Result subfolder of plate: ' + plate);
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
            logger.error('Failed to read feature: ' + file);
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
 * Archive result back to online storage
 */
function feedbackCSV(plate, callback) {
  g_blob.putBlockBlobFromFile(g_container, plate + '.csv', g_tmpFolder + plate + '.csv', function (err, blob) {
    if (err) {
      logger.error('Failed to upload CSV file');
      return callback(err);
    }
    
    logger.info('CSV result uploaded');
    callback(err, plate);
  });
}

/**
 * Archive FITC folder back to online storage
 */
function feedbackFITC(plate, callback) {
  var fs = require('fs');
  var async = require('async');

  // list
  fs.readdir(g_tmpFolder + plate + '/FITC', function (err, files) {
    if (err) {
      logger.error('Failed to list FITC subfolder of plate: ' + plate);
      return callback();
    }
    
    async.eachLimit(files, g_currenency, function (file, callback) {
      g_blob.putBlockBlobFromFile(g_container, plate + '_FITC/' + file, g_tmpFolder + plate + '/FITC/' + file, function (err, blob) {
        if (err) {
          logger.warn('Failed to upload FITC file: ' + file);
        }
        callback(err, plate);
      });
    }, function(err) {
      // if any of the saves produced an error, err would equal that error
      return callback(err, plate);
    });
  });
  
}

/**
 * helper function to copy file
 */
function copyFile(source, target, cb) {
  var fs = require('fs');
  var cbCalled = false;

  var rd = fs.createReadStream(source);
  rd.on("error", done);

  var wr = fs.createWriteStream(target);
  wr.on("error", done);
  wr.on("close", function(ex) {
    done();
  });
  rd.pipe(wr);

  function done(err) {
    if (!cbCalled) {
      cb(err);
      cbCalled = true;
    }
  }
}

/**
 * Archive debug logger back to online storage
 */
function feedbackLogger(plate, callback) {
  copyFile(g_tmpFolder + 'debug.log', g_tmpFolder + plate + '.log', function (err) {
    if (err) {
      logger.error('Failed to copy debug log: ' + err);
      return callback();
    }
    g_blob.putBlockBlobFromFile(g_container, plate + '.log', g_tmpFolder + plate + '.log', function (err, blob) {
      if (err) {
        logger.error('Failed to upload debug log: ' + err);
        return callback();
      }

      logger.info('Debug log uploaded');
      callback(err, plate);
    });
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
      feedbackCSV, // upload result back to Azure blob
      feedbackFITC, // upload FITC result to Azure blob
      feedbackLogger // upload debug logger to Azure blob 
    ],
    function(err, results) {
      if (err) {
        logger.error('Failed to complete plate, error: ' + err);
        return callback(err);
      }
      logger.info('>>>>>>> Plate process complete: ' + plate);
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

  logger.info('Process plate: ' + plate);
  // only handle plate folder, not other file or folder with other subfix.
  g_blob.listBlobs(g_container, {prefix: plate + '/' /*, include: 'metadata'*/}, function(err, blobs) {
    if (err) {
      logger.error('Failed to access Azure Blob: ' + err);
      return callback(err);
    }
    
    if (!blobs || blobs.length === 0) {
      logger.warn('Failed to fetch plate images: ' + plate);
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
      //logger.info(blob.name);
      //logger.info(blob.metadata);
      //logger.info(blob.properties);
      
      if (S(blob.name).contains('enhanced') || !S(blob.name).endsWith('.tif')) {
        return callback(); // ignore this file silently
      }
      
      var n = blob.name.lastIndexOf('/');
      if (n === -1) {
        logger.warn('Invalid blob path: ' + blob.name);
        return callback();
      }
      var filename = blob.name.substr(n+1);
      
      fs.stat(g_tmpFolder + blob.name, function(err, stats) {
        if (err || stats.size != blob.properties['content-length']) {
          // file not ready, fetch file to local temporary storage
          logger.info('Download ' + blob.name);
          // truncate intermedia subfolders
          g_blob.getBlobToFile(g_container, blob.name, g_tmpFolder + plate + '/' + filename, callback);
        } else {
          return callback(); // skip file download
        }
      });
      
    }, function (err, results) {
      if (err) {
        logger.error('Failed to fetch plate image: ' + err);
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
      logger.info(err);
      process.exit(1);
    }
    
    //shutdown this compute node
    client.virtualMachines.shutdown(g_serviceName, data.deployments[0].name,
        os.hostname(), {postShutdownAction: 'StoppedDeallocated'}, function (err) {
      if (err) {
        logger.error(err);
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
  var os = require('os');
  
  g_currenency = os.cpus().length; // change threads to # of cpu cores

  // create temp folder
  if (!fs.existsSync(g_tmpFolder)) {
    fs.mkdirSync(g_tmpFolder);
  }
  
  if (version === 1) {
    /****************************
     * V1: decide plates to process by a plate.json file, not scale-able
     ****************************/
    logger.info('Fetching plate.json ...');
    g_blob.getBlobToFile(g_container, 'plate.json', g_tmpFolder + 'plate.json', function (err, blob) {
      if (err) {
        logger.error('No plate list to be processed, or error occured to fetch it: ' + err);
        return callback(err);
      }
      logger.info('plate.json fetched');
      
      var plates = require(g_tmpFolder + 'plate.json');
      if (!plates || !Array.isArray(plates)) {
        logger.error('failed to load plate.json');
        return callback('no data');
      }
      
      async.eachSeries(plates, fetchPlate, callback);
    });
  } else if (version === 2) {
    /****************************
     * V2: decide plates to process by Azure Blob Queue, support multiple instance 
     ****************************/
    logger.info('Fetching plate message queue ...');
    var mq = azure.createQueueService(g_storageAccount, g_storageAcessKey);
    var hasData = true;

    mq.createQueueIfNotExists('plates', function (err) {
      if (err) {
        logger.error('Failed to create queue: ' + err);
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
            logger.error('Failed to fetch message queue: ' + err);
            return callback(err);
          }
          //logger.info(serverMessages);
          if (!message || !message.length) {
            // no message to process
            hasData = false;
            logger.warn('No plate to process');
            return callback('no data');
          }
          fetchPlate(message[0].messagetext, function(err) {
            if (!err) {
              // when a plate is processed competely, remove it from MQ
              mq.deleteMessage('plates', message[0].messageid, message[0].popreceipt, function(err) {
                if (err) {
                  logger.info('Failed to remove message: ' + err);
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

