'use strict';
var zlib = require('zlib');
let extend = require('extend');
let _ = require('lodash');
let AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
let firehose = new AWS.Firehose();
var PromiseBB = require("bluebird");

console.log('Loading function');

exports.handler = (event, context, callback) => {
  // console.log(JSON.stringify(event))
  // If it has records
  if (event.Records) {
    var output = {firehose: {
       "DeliveryStreamName": "es-tractor-orc-flight-availability-response",
       "Records": []
    }};

    _.forEach(event.Records, function(evtRecord) {
      var evtSrc = evtRecord.eventSource;
      if (evtRecord.EventSource) {
        evtSrc = evtRecord.EventSource;
      }
      switch(evtSrc) {
        case "aws:kinesis":
          /* Process the list of records and transform them */
          var parsedData = JSON.parse(new Buffer(evtRecord.kinesis.data, "base64"))
          if (parsedData.data) {
            try {
              var unziped = zlib.unzipSync(new Buffer(parsedData.data, "base64"))
              var additionalData = JSON.parse(unziped)
              delete parsedData.data
              extend(additionalData, parsedData)

    					output.firehose["Records"].push({ "Data": JSON.stringify(additionalData) });
            } catch (err) {
              console.log(`Error occurred during unzip ${err}`);
            }
          }
          break;
        default:
          console.log(`Event source "${evtRecord.eventSource}" is not supported. Event: ${JSON.stringify(evtRecord)}`)
          callback(null, "Success");
      }
    });

    var records = _.chunk(output.firehose["Records"], 5);

    PromiseBB.map(records, function(chunk) {
      return new Promise((resolve, reject) => {
        firehose.putRecordBatch({
           "DeliveryStreamName": "es-tractor-orc-flight-availability-response",
           "Records": chunk
        }, function(err, data) {
          if (err) {
            console.log("Occurred an error: ", err.stack);
            resolve("Success");
          } else {
            if (data["FailedPutCount"] && data["FailedPutCount"] > 0) {
              console.log(`Some records wasn't delivered, a total of ${data["FailedPutCount"]}. ${JSON.stringify(data)}`)
            }
            console.log(`Was sent to firehose ${chunk.length} records`)
            resolve("Success");
          }
        });
      });
    }, {
      concurrency: parseInt(process.env.CONCURRENCY)
    }).then((result) => {
      callback(null, "Success")
    }).catch(err => {
      console.log(`Occurred an error ${JSON.stringify(err)}`)
      callback(null, "Success")
    })
  }
};
