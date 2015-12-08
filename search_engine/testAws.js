/**
 * Created by adamcole on 12/7/15.
 */
var AWS = require('aws-sdk');

AWS.config.update({
    region: "us-west-2",
    endpoint: "dynamodb.us-west-2.amazonaws.com"
});

var dynamodbDoc = new AWS.DynamoDB.DocumentClient();


/**
 * PUT ITEM
 */
//var params = {
//    TableName: "uiTest",
//    Item: {
//        "url": "google.com",
//        "rank":.3
//    }
//};
//
//dynamodbDoc.put(params, function(err, data) {
//    if (err) {
//        console.error("Error JSON:", JSON.stringify(err, null, 2));
//    } else {
//        console.log("PutItem succeeded!");
//    }
//});

/**
 * GET ITEM
 */
//var params = {
//    TableName: 'pagerank-results',
//    Key : {
//        url: "j"
//    }
//};
//
//dynamodbDoc.get(params, function(err, data) {
//    if (err) {
//        console.log(err); // an error occurred
//    }
//    else {
//        console.log(data); // successful response
//    }
//});
