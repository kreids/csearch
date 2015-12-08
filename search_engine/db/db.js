/**
 * Created by adamcole on 12/8/15.
 */
var AWS = require('aws-sdk');
AWS.config.update({
    region: "us-west-2",
    endpoint: "dynamodb.us-west-2.amazonaws.com"
});

var dynamodb = new AWS.DynamoDB();
var dynamodbDoc = new AWS.DynamoDB.DocumentClient();

var PAGE_RANK_TABLE_NAME = "pagerank-results";
var PAGE_RANK_KEY_NAME = "url";

var TFIDF_TABLE_NAME = "InverseIndex";
var TFIDF_KEY_NAME = "word";


//var params = {
//    TableName: TFIDF_TABLE_NAME /* required */
//};
//
//dynamodb.describeTable(params, function(err, data) {
//    if (err) console.log(err, err.stack); // an error occurred
//    else     console.log(JSON.stringify(data));           // successful response
//});

exports.getTfIdfs = function(words, callback) {

    var wordKeyValPairs = [];
    words.forEach(function(word) {
        var obj = {};
        obj[TFIDF_KEY_NAME] = word;
        wordKeyValPairs.push(obj);
    });

    var params = {
        RequestItems: {}
    };

    params.RequestItems[TFIDF_TABLE_NAME] = {Keys: wordKeyValPairs};

    dynamodbDoc.batchGet(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else     console.log(JSON.stringify(data.Responses[TFIDF_TABLE_NAME]));           // successful response
    });
};
//
//var getPageRank = function(urls, callback) {
//    var params = {
//        "RequestItems": {
//            PAGE_RANK_TABLE_NAME: {
//                Keys: [urls]
//            }
//        }
//    }
//
//    dynamodbDoc.get(params, function(err, data) {
//        callback(err, data)
//    });
//}

exports.getPageRanks = function(urls, callback) {

    var urlKeyValPairs = [];
    urls.forEach(function(url) {
        var obj = {};
        obj[PAGE_RANK_KEY_NAME] = url;
        urlKeyValPairs.push(obj);
    });

    var params = {
        RequestItems: {
            'pagerank-results': {
                Keys: [{url: "j"},{url: "i"},{url: "h"}]
            }
        }
    };

    console.log(JSON.stringify(params));

    dynamodbDoc.batchGet(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else     console.log(data.Responses[PAGE_RANK_TABLE_NAME]);           // successful response
    });
};
