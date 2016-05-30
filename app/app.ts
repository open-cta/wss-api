/// <reference path='../typings/index.d.ts' />
'use strict'

/**
 * Configure environmental variables
 */
require('dotenv').config({path: '.secrets'})

/**
 * Setting up modules
 */
const _ = require('underscore'),
     promise = require('bluebird'),
     winston  = require('winston'),
     delay = require('delay'),
     AWS = require('aws-sdk'),
     WebSocketServer = require('ws').Server,
     wss = new WebSocketServer({ port: 4200 }); // ayy lmao 

var iterating = false;

/**
 * Configure logging
 */
var logger = new (winston.Logger)({
  transports: [
    new (winston.transports.Console)({ level: 'debug' })
  ]
})

/**
 * Set up AWS
 */
AWS.config.update({region: 'us-east-1'});
var ddb = new AWS.DynamoDB.DocumentClient(),
    dynamo =  promise.promisifyAll(ddb);

/**
 * Functions for retrieving data from DB
 */

// get all items in the run number given a hash
// if following is true, get only future predictions from the timestamp
async function getAllOfRunNumber(hash, following) {
  // calculate a range of values. run numbers are per day,
  // so this gets us a list of all other predictions given the run number and a timestamp

  let unixTime = Number(hash.prdt) * 1000

  let min = new Date(unixTime)
  min.setHours(0, 0, 0)

  let max = new Date(unixTime) // have to set this again here to get us another object to operate on
  max.setHours(0, 0, 0)
  max.setDate(max.getDate() + 1)

  // only get the following predictions
  if (following) {
    min = new Date(unixTime)
  }

  var params = {
      TableName : 'cta-trains',
      KeyConditionExpression: 'rn = :rn and prdt BETWEEN :min AND :max',
      ExpressionAttributeValues: {
          ':min': min.getTime() / 1000,
          ':max': max.getTime() / 1000,
          ':rn': Number(hash.rn)
      }
  };

  try {
    let query = await dynamo.queryAsync(params)
    return query.Items
  } catch (error) {
    logger.warning(error)
  }
};

// Get a train prediction by it's hash (partition - rn number and sort key - prdt)
async function getTrainByHash(hash) {
  var params = {
      TableName : 'cta-trains',
      KeyConditionExpression: 'prdt = :prdt and rn = :rn',
      ExpressionAttributeValues: {
          ':prdt': hash[0].prdt, // need to handle cases of multiple trains
          ':rn': hash[0].rn
      }
  };

  try {
    let query = await dynamo.queryAsync(params)
    return query.Items
  } catch (error) {
    logger.warning(error)
  }
}

// get a hash (patition/sort key) by searching the index
async function trainHashAtTimestamp(timestamp) {
  var params = {
      TableName : 'cta-trains',
      IndexName: 'prdt-index',
      KeyConditionExpression: 'prdt = :prdtime',
      ExpressionAttributeValues: {
          ':prdtime': timestamp
      }
  };

  try {
     let query = await dynamo.queryAsync(params)
     return query.Items
  } catch (error) {
    logger.warning(error)
  }
}

// essentially just run both of the two above functions together
async function getTrainsByTimestamp(timestamp) {
  try {
    let trainsHash = await trainHashAtTimestamp(timestamp);
    if (trainsHash.length > 0) {
      let trains = await getTrainByHash(trainsHash);
      return trains
    }
  } catch (error) {
    logger.warning(error)
  }
}

// get a random train prediction and send them to websockets
async function getRandomPredictions(ws) {
    let min = 1460592000
    let max = Math.floor(Date.now() / 1000)
    let ts = _.random(min, max)
    let hash = await trainHashAtTimestamp(ts)
    if (_.isEmpty(hash)) {
      getRandomPredictions(ws);
    } else {
      let trains = await getTrainByHash(hash)
      ws.send(JSON.stringify(trains[0]))
      let timestamp = ts++
      iterating = true
      iterate(ws, timestamp)
    }
}


// start with a timestamp and just keep working into THE FUTURE
async function iterate(ws, timestamp) {
  try {
    for (var ts = timestamp; iterating; ts++) {
      let trains = await getTrainsByTimestamp(ts)
      if (typeof trains !== 'undefined') {
        ws.send(JSON.stringify(trains[0]))
      }
    }
  } catch (error) {
    logger.warning(error)
  }
}

/**
 * Websockets stuff
 */

function toEvent (message) {
  try {
    var event = JSON.parse(message)
    this.emit(event.type, event.payload)
  } catch (err) {
    console.log('not an event' , err)
  }
}

wss.on('connection', function(ws) {

  ws.on('message', toEvent);

  ws.on('follow', async function (data) {
     iterating = false
     let arr = await getAllOfRunNumber(data, true)
     let dly = 0

     _.each(arr, async function(newData, i, arr) {
          if (i > 0) {
            let previousIndex = i - 1
            let previousData = arr[previousIndex]
            let d = (newData.prdt - previousData.prdt)*1000
            dly = dly + d
            await delay(dly)
          }
          ws.send(JSON.stringify(newData))
     });
  });

  ws.on('iterate', function (data) {
     iterating = true
     iterate(ws, Number(data.timestamp))
     // this sends data down, but because of the for loop, we're not doing ws.send here
  });

  ws.on('random', function (data) {
    iterating = false
    getRandomPredictions(ws)
  });

});

wss.on('close', function close() {
  iterating = false
  console.log('disconnected')
});
