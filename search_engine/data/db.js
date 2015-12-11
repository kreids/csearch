/**
 * Created by adamcole on 12/8/15.
 */
var jsdom = require('jsdom').jsdom;
var document = jsdom('<html></html>', {});
var window = document.defaultView;
var $ = require('jquery')(window);
var AWS = require('aws-sdk');

/**
 * db provides access to the DynamoDB tables for tfidf, pagerank, and titles
 */


AWS.config.update({
    region: "us-west-2",
    endpoint: "dynamodb.us-west-2.amazonaws.com"
});

var dynamodb = new AWS.DynamoDB();
var dynamodbDoc = new AWS.DynamoDB.DocumentClient();

exports.PAGE_RANK_TABLE_NAME = "pagerank-results";
exports.PAGE_RANK_KEY_NAME = "url";

exports.TFIDF_TABLE_NAME = "index1";
exports.TFIDF_KEY_NAME = "word";
exports.TFIDF_ITEM_COUNT = 1078;

exports.TITLES_TABLE_NAME = "titles";
exports.TITLES_KEY_NAME = "url";


//var params = {
//    TableName: exports.TFIDF_TABLE_NAME /* required */
//};
//
//dynamodb.describeTable(params, function(err, data) {
//    if (err) console.log(err, err.stack); // an error occurred
//    else     console.log(data.Table.ItemCount);           // successful response
//});

// takes array of words

exports.getAllPageTfIdfs = function(words) {
    var deferreds = [];
    var intervals = Math.floor(words.length / 100);
    for (var i = 0; i < intervals + 1; i++) {
        var endSlice = ((i+1)*100 > words.length)? words.length : (i+1)*100;
        var wordsSlice = words.slice(i*100,endSlice);
        deferreds.push(exports.getTfIdfs(wordsSlice));
    }
    return $.when.apply($, deferreds);
};

exports.getTfIdfs = function(words) {
    var deferred = $.Deferred();

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
        deferred.resolve({
            err: err,
            data: data
        });           // successful response
    });
    return deferred;
};


exports.getAllPageRanks = function(urls) {
    var deferreds = [];
    var intervals = Math.floor(urls.length / 100);
    for (var i = 0; i < intervals + 1; i++) {
        var endSlice = ((i+1)*100 > urls.length)? urls.length : (i+1)*100;
        var urlsSlice = urls.slice(i*100,endSlice);
        deferreds.push(exports.getPageRanks(urlsSlice));
    }
    return $.when.apply($, deferreds);
};


exports.getPageRanks = function(urls) {
    var deferred = $.Deferred();
    var urlKeyValPairs = [];
    urls.forEach(function(url) {
        var obj = {};
        obj[exports.PAGE_RANK_KEY_NAME] = url;
        urlKeyValPairs.push(obj);
    });

    var params = {
        RequestItems: {}
    };
    params.RequestItems[exports.PAGE_RANK_TABLE_NAME] = {Keys: urlKeyValPairs};

    dynamodbDoc.batchGet(params, function(err, data) {
        if (err) console.log(err, err.stack);
        deferred.resolve({
            err: err,
            data: data
        });           // successful response
    });
    return deferred;
};

exports.getAllPageTitles = function(titles) {
    var deferreds = [];
    var intervals = Math.floor(titles.length / 100);
    for (var i = 0; i < intervals + 1; i++) {
        var endSlice = ((i+1)*100 > titles.length)? titles.length : (i+1)*100;
        var titleSlice = titles.slice(i*100,endSlice);
        deferreds.push(exports.getPageTitles(titleSlice));
    }
    return $.when.apply($, deferreds);
};

exports.getPageTitles = function(titles) {
    var deferred = $.Deferred();

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
        deferred.resolve({
            err: err,
            data: data
        });
    });
    return deferred;
};