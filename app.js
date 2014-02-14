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
function enhanceImage(path, callback) {
  var spawn = require('child_process').spawn;
  var preprocessor = spawn('java', ['-jar', '../PeroxiTracker_Standalone/PeroJava.jar', path]);

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
}

/**
 * Content screening: Feature set calculation & classification 
 */
function classifyImage(path, callback) {
  
}


/**
 * Archive result back to online storage, and mark file processed
 */
function feedbackBlob(path, callback) {
  
}

/**
 * Process a plate 
 */
function processPlate(plate) {
  var async = require('async');

  // step 1: enhance FITC files and save to FITC folder
  
  // step 2: enhance DAPI files and save to DAPI folder
  
  // step 3: count # of cells
  
  // step 4: calculate tophat of wells
  
  // step 5: calculate histogram
  
  // step 6: calculate feature set 
  
  // step 7: consolidate CSV file
  
  
  /*
  async.series([
      function(callback) {
        enhanceImage(g_tmpFolder + blob.blob, callback);
        //listAzureBlobs();
      //enhanceImage('/tmp/Sandbox/PBD GFP-Hoechst_LOPAC_2_1/A - 1(fld 1 wv DAPI - DAPI).tif');

      },
      function(callback) {
        // do some more stuff ...
        callback(null, 'two');
      }
    ],
    function(err, results){
        // results is now equal to ['one', 'two']
    }
  );
  */
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
