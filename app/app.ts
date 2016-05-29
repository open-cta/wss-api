/// <reference path='../typings/index.d.ts' />
'use strict'

/**
 * Configure environmental variables
 */
require('dotenv').config({path: '.secrets'})

/**
 * Setting up modules
 */
var _ = require('underscore'),
    promise = require('bluebird'),
    winston  = require('winston'),
    AWS = require('aws-sdk'),
    WebSocketServer = require('ws').Server,
    wss = new WebSocketServer({ port: 4200 }),
    clientConnected = false;

/**
 * Configure logging
 */
var logger = new (winston.Logger)({
  transports: [
    new (winston.transports.Console)({ level: 'debug' }),
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

// Get a train by it's hash (partition and sort key)
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
};

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
};

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

// get some random trains and send them to websockets
async function getRandomTrains(ws) {
    let min = 1460592000
    let max = Math.floor(Date.now()/1000)
    let ts = _.random(min, max)
    let hash = await trainHashAtTimestamp(ts)
    if (_.isEmpty(hash)) {
      getRandomTrains(ws);
    } else {
      let trains = await getTrainByHash(hash)
      ws.send(JSON.stringify(trains[0]))
      let timestamp = ts++
      iterate(ws, timestamp)
    }
}

// start with a timestamp and just keep working into THE FUTURE
async function iterate(ws, timestamp) {
  try {
    for (var ts = timestamp; clientConnected; ts++) {
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

wss.on('connection', async function connection(ws) {
  clientConnected = true
  // by default just get some random trains
  getRandomTrains(ws)

  // if we get a message, go out find it and start iterating over that timestamp. 
  // probably need to do better existence checking here
  ws.on('message', function incoming(message) {
      iterate(ws, Number(message)) // just assume that the message is a timestamp for now
   });
});

wss.on('close', function close() {
  clientConnected = false;
  console.log('disconnected');
});
