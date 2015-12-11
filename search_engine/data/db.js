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

exports.PAGE_RANK_TABLE_NAME = "pagerank-test2";
exports.PAGE_RANK_KEY_NAME = "url";

exports.TFIDF_TABLE_NAME = "indextest2";
exports.TFIDF_KEY_NAME = "word";

exports.TITLES_TABLE_NAME = "titles";
exports.TITLES_KEY_NAME = "url";

//
//var params = {
//    TableName: "indextest2" /* required */
//};
//
//dynamodb.describeTable(params, function(err, data) {
//    if (err) console.log(err, err.stack); // an error occurred
//    else     console.log(JSON.stringify(data));           // successful response
//});

// takes array of words
exports.getTfIdfs = function(words, callback) {
    var wordKeyValPairs = [];
    words.forEach(function(word) {
        var obj = {};
        obj[exports.TFIDF_KEY_NAME] = word;
        wordKeyValPairs.push(obj);
    });

    var params = {
        RequestItems: {}
    };

    params.RequestItems[exports.TFIDF_TABLE_NAME] = {Keys: wordKeyValPairs};

    dynamodbDoc.batchGet(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        callback(err, data);           // successful response
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
        obj[exports.PAGE_RANK_KEY_NAME] = url;
        urlKeyValPairs.push(obj);
    });

    //var params = {
    //    RequestItems: {
    //        'pagerank-test2': {
    //            Keys: [{url: "http://www.theguardian.com/business/currencies"}]
    //        }
    //    }
    //};

    var params = {
        RequestItems: {}
    };

    params.RequestItems[exports.PAGE_RANK_TABLE_NAME] = {Keys: urlKeyValPairs};

    dynamodbDoc.batchGet(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        callback(err,data);           // successful response
    });
};

exports.getPageTitles = function(titles, callback) {

    var KeyValPairs = [];
    titles.forEach(function(url) {
        var obj = {};
        obj[exports.TITLES_KEY_NAME] = url;
        KeyValPairs.push(obj);
    });

    var params = {
        RequestItems: {}
    };

    params.RequestItems[exports.TITLES_TABLE_NAME] = {Keys: KeyValPairs};

    dynamodbDoc.batchGet(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        callback(err,data);
    });
};