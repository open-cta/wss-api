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
    winston  = require('winston'),
    WebSocketServer = require('ws').Server;


async function getTrains(){
  var AWS = require('aws-sdk');
  AWS.config.update({region: 'us-east-1'});
  var docClient = new AWS.DynamoDB.DocumentClient();

  var params = {
      TableName : 'cta-trains',
      IndexName: 'prdt-index',
      KeyConditionExpression: 'prdt = :prdtime',
      ExpressionAttributeValues: {
          ':prdtime': 1460808468
      }
  };

  docClient.query(params, function(err, data) {
      if (err) {
          console.error('Unable to query. Error:', JSON.stringify(err, null, 2));
      } else {
          console.log('Query succeeded.');
          console.log(data)
      }
  });

};

getTrains();

/**
 * Configure logging
 */
var logger = new (winston.Logger)({
  transports: [
    new (winston.transports.Console)({ level: 'debug' }),
    new (winston.transports.File)({ filename: '../.logs/app.log', level: 'info' })
  ]
})
