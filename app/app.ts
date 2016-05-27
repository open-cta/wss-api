/// <reference path='../typings/index.d.ts' />
'use strict'

/**
 * Configure environmental variables
 */
require('dotenv').config({path: '.secrets'})

/**
 * Setting up Modules
 */
var _ = require('lodash'),
    promise = require('bluebird'),
    winston  = require('winston'),
    AWS = require('aws-sdk'),
    WebSocketServer = require('ws').Server;

AWS.config.update({region: 'us-east-1'});

var ddb = new AWS.DynamoDB.DocumentClient(),
    dynamo =  promise.promisifyAll(ddb);

async function getTrainByHash(hash) {
  var params = {
      TableName : 'cta-trains',
      KeyConditionExpression: 'prdt = :prdt and rn = :rn',
      ExpressionAttributeValues: {
          ':prdt': hash[0].prdt,
          ':rn': hash[0].rn
      }
  };

  try {
    let query = await dynamo.queryAsync(params)
    return query.Items
  } catch (error) {
    console.log(error)
  }
};

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
    console.log(error)
  }

};

async function getTrainsByTimestamp() {
  try{ 
    let trainsHash = await trainHashAtTimestamp(1460808468);
    let trains = await getTrainByHash(trainsHash);
    return trains
  } catch (error) {
    console.log(error)
  }
}

main();

/**
 * Configure logging
 */
var logger = new (winston.Logger)({
  transports: [
    new (winston.transports.Console)({ level: 'debug' }),
    new (winston.transports.File)({ filename: '../.logs/app.log', level: 'info' })
  ]
})
