// global variables
var g_storageAccount = 'bioimage';
var g_storageAcessKey = 't2cCFG4nKcwSp4NrnghpI9fnZZ3hR8YvEYshRocCAzXJ5u3dSEx+b5sA05URmKk1MOFwVwStHa+d1la6TMauxA==';
var g_tmpFolder = '/tmp/';
var g_container = 'images';

var azure = require('azure');
var g_blob = azure.createBlobService(g_storageAccount, g_storageAcessKey);

/**
 * Preprocessing: enhance image's quality  
 */
function enhanceImage(plate, callback) {
  var fs = require('fs');
  var async = require('async');
  var spawn = require('child_process').spawn;
  
  // list
  fs.readdir(g_tmpFolder + plate, function (err, files) {
    if (err) {
      console.log('Failed to list directory: ' + plate);
      return callback(err);
    }
    
    async.each(files, function (file, callback) {
      var preprocessor = spawn('java', 
          ['-jar', '../PeroxiTracker_Standalone/PeroJava.jar', // jar path
           g_tmpFolder + plate + '/' + file]); // input filepath

      // debug purpose
      preprocessor.stdout.on('data', function (data) {
        console.log('> ' + data);
      });

      // debug purpose
      preprocessor.stderr.on('data', function (data) {
        console.log('>> ' + data);
      });

      // handle exit code
      preprocessor.on('close', function (code) {
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
  
  // list
  fs.readdir(g_tmpFolder + plate + '/DAPI', function (err, files) {
    if (err) {
      console.log('Failed to list DAPI subfolder of plate: ' + plate);
      return callback(err);
    }
    
    async.each(files, function (file, callback) {
      var well = file.match(pattern);
      if (!well) {
        // not matched file name, ignore it;
        console.log('Invalid filename format, ignored: ' + file);
        return callback(); // no argument imply silent failback to next async.each
      }

      var preprocessor = spawn('../PeroxiTracker_Matlab/onewellCellCounting.exe', // program path
          [g_tmpFolder + plate + '/DAPI/' + file, // input file path
           g_tmpFolder + plate + '/Result/' + well[0] + '_' + well[1] + '_cell_obj_cords.txt']); // output file path

      // debug purpose
      preprocessor.stdout.on('data', function (data) {
        console.log('> ' + data);
      });

      // debug purpose
      preprocessor.stderr.on('data', function (data) {
        console.log('>> ' + data);
      });

      // handle exit code
      preprocessor.on('close', function (code) {
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
  
  // list
  fs.readdir(g_tmpFolder + plate + '/FITC', function (err, files) {
    if (err) {
      console.log('Failed to list FITC subfolder of plate: ' + plate);
      return callback(err);
    }
    
    async.map(files, function (file, callback) {
      var well = file.match(pattern);
      if (!well) {
        // not matched file name, ignore it;
        console.log('Invalid filename format, ignored: ' + file);
        return callback(); // no argument imply silent success
      }

      var preprocessor = spawn('../PeroxiTracker_Matlab/onewellTophat.exe', // program path
          [g_tmpFolder + plate + '/FITC/' + file, // input file path
           g_tmpFolder + plate + '/Tophat/' + well[0] + '_' + well[1] + '_tophat.mat']); // output tophat file path

      // debug purpose
      preprocessor.stdout.on('data', function (data) {
        console.log('> ' + data);
      });

      // debug purpose
      preprocessor.stderr.on('data', function (data) {
        console.log('>> ' + data);
      });

      // handle exit code
      preprocessor.on('close', function (code) {
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
  var preprocessor = spawn('../PeroxiTracker_Matlab/onePlateHistCalc.exe', // program path
      [g_tmpFolder + plate + '/Tophat', found]); // input path & union result of step 4 
  // implicit output is Tophat/netHist.mat

  // debug purpose
  preprocessor.stdout.on('data', function (data) {
    console.log('> ' + data);
  });

  // debug purpose
  preprocessor.stderr.on('data', function (data) {
    console.log('>> ' + data);
  });

  // handle exit code
  preprocessor.on('close', function (code) {
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
  
  // list
  fs.readdir(g_tmpFolder + plate + '/Tophat', function (err, files) {
    if (err) {
      console.log('Failed to list Tophat subfolder of plate: ' + plate);
      return callback(err);
    }
    
    async.each(files, function (file, callback) {
      var well = file.match(pattern);
      if (!well) {
        // not matched file name, ignore it;
        console.log('Invalid filename format, ignored: ' + file);
        return callback(); // no argument imply silent failback to next async.each
      }

      var preprocessor = spawn('../PeroxiTracker_Matlab/onewellFeatGen.exe', // program path
          [g_tmpFolder + plate + '/Tophat/' + file, // input file path
           g_tmpFolder + plate + '/Result/' + well[0] + '_' + well[1] + '_feature.txt', // output file path
           g_tmpFolder + plate + '/Tophat/netHist.mat']); // input file from implicit output of step 5 

      // debug purpose
      preprocessor.stdout.on('data', function (data) {
        console.log('> ' + data);
      });

      // debug purpose
      preprocessor.stderr.on('data', function (data) {
        console.log('>> ' + data);
      });

      // handle exit code
      preprocessor.on('close', function (code) {
        callback( code !== 0 ? code : null);
      });
    }, function(err) {
      // if any of the saves produced an error, err would equal that error
      return callback(err, plate);
    });
  });
}

/**
 * Archive result back to online storage, and mark file processed
 */
function feedbackBlob(path, callback) {
  
}

/**
 * After images of a plate has been downloaded to local disk, process these images 
 */
function processPlate(plate) {
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
      //genCSV, // step 7: consolidate CSV file
    ],
    // optional callback
    function(err, results) {
      // results is now equal to ['one', 'two']
    });
}

/**
 * Fetch a plate
 */
function fetchPlate(plate) {
  var S = require('string');
  var fs = require('fs');
  var async = require('async');

  console.log('Process plate: ' + plate);
  g_blob.listBlobs(g_container, {prefix: plate /*, include: 'metadata'*/}, function(error, blobs) {
    if (error) {
      console.error('Failed to access Azure Blob: ' + error);
      process.exit(1);
    }
    
    if (!blobs || blobs.length === 0) {
      console.warn('Failed to fetch plate images: ' + plate);
      return;
    }
    
    // create temp folder
    fs.exists(g_tmpFolder + plate, function (exists) {
      if (!exists) {
        fs.mkdir(g_tmpFolder + plate);
      }
    });
    
    // Async download file in parallel
    async.map(blobs, function (blob, callback) {
      //console.log(blob.name);
      //console.log(blob.metadata);
      //console.log(blob.properties);
      
      if (S(blob.name).contains('enhanced') || !S(blob.name).endsWith('.tif')) {
        return callback(); // ignore this file silently
      }
      
      // fetch file to local temporary storage
      g_blob.getBlobToFile(g_container, blob.name, g_tmpFolder + blob.name, callback);
    }, function (err, results) {
      if (err) {
        console.error('Failed to fetch plate image: ' + error);
        return;
      }
      
      processPlate(plate);
    });
  });
}

/**
 *  main function
 */
function main() {  
  // fetch plate list to be processed
  g_blob.getBlobToFile(g_container, 'plate.json', g_tmpFolder + 'plate.json', function (error, blob) {
    if (error) {
      console.log('No plate list to be processed, or error occured to fetch it: ' + error);
      return;
    }
    
    var plates = require(g_tmpFolder + 'plate.json');
    if (!plates || !Array.isArray(plates)) {
      console.log('failed to load plate.json');
      return;
    }
    
    for (var i=0; i< plates.length; i+=1) {
      fetchPlate(plates[i]);
    }
  });
  
}

main();
/*
enhanceImage('LOPAC', function (err) {
  console.log(err);
});
*/
