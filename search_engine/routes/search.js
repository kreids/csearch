/**
 * Created by adamcole on 12/7/15.
 */
var express = require('express');
var router = express.Router();
var db = require('../data/db');
var jsdom = require('jsdom').jsdom;
var document = jsdom('<html></html>', {});
var window = document.defaultView;
var $ = require('jquery')(window);

/**
 * search gets /search routes. It takes queries passed in the url, processes
 * them by looking up the word's tfidf scores and pageranks, uses an algorithm
 * to rank the urls and returns the ranked data to the client.
 */


/* GET search results. */
router.get('/', function(req, res, next) {
    var query = req.query.searchQuery.toLowerCase().trim();
    var deferred = $.Deferred();
    deferred.done(function (rankedPages) {
        rankedPages.query = req.query.searchQuery;
        res.send({
            type: "results",
            query: req.query.searchQuery,
            rankedPages: rankedPages
        });
    }).fail(function(err) {
        console.log("fail");
        res.send({
            type: "fail",
            query: req.query.searchQuery
        });
    });
    getSearchResults(query, deferred);
});

/* getUrlList returns the urls in the tfidfObj */
var getUrlList = function(tfidfObj) {
    var urls = [];
    tfidfObj.mapList.forEach(function (urlScorePair) {
       urls.push(urlScorePair.url);
    });

    var uniqueUrls = [];
    $.each(urls, function(i, el){
         uniqueUrls.push(el);
    });
    return uniqueUrls;
};


var getWordFrequency = function(word, arr) {
    var count = 0;
    arr.forEach(function(arrVal) {
       if (word === arrVal) count++;
    });
    return count;
};

var calcTermFrequencyScore = function(queryWords) {
    var tfScores = {};
    queryWords.forEach(function(word) {
        var wordFreq =  getWordFrequency(word, queryWords);
        tfScores[word] = wordFreq/queryWords.length;
    });
    return tfScores;
};

var calcQueryIdf = function(queryWords, tfidfData, urls) {
    var idfScores = {};
    queryWords.forEach(function(word) {
        var tfidfEntry = $.grep(tfidfData, function (e) {
            return e.word === word;
        });
        var docCount = tfidfEntry.length > 0 ? tfidfEntry[0].mapList.length : 0;
        if (docCount === 0) var idfScore = 0;
        else var idfScore = 1 + Math.log(db.TFIDF_ITEM_COUNT / docCount);
        idfScores[word] = idfScore;
    });
    return idfScores;
};

var dotProduct = function(a,b) {
    var n = 0;
    var lim = Math.min(a.length, b.length);
    for (var i = 0; i < lim; i++) n += a[i] * b[i];
    return n;
};

function normalize(a) {
    var len = length(a);
    for (var i = 0; i < a.length; i++) {
        a[i] = a[i] / len;
    }
    return a;
}

function length(a) {
    var sumsqr = 0;
    for (var i = 0; i < a.length; i++) sumsqr += a[i]*a[i];
    return Math.sqrt(sumsqr);
};

var similarity = function(a, b) {
    return dotProduct(normalize(a), normalize(b));
};

var isOneTermQuery = function(querytfIdfsVec) {
    var termsWithVal = 0;
    querytfIdfsVec.forEach(function(val) {
        if (val > 0) termsWithVal++;
    });
    return termsWithVal > 1;
};

var getOneTermVal = function(doctfIdfsVec) {
    for (var i = 0; i < doctfIdfsVec.length; i++) {
        if (doctfIdfsVec[i] > 0) return doctfIdfsVec[i];
    }
    return 1;
};

var calculateCosineSimilarity = function(queryWords, queryTfs, queryIdfs, tfidfData, urls) {
    var cosineScores = {};
    var querytfIdfsVec = [];
    for (var i = 0; i < queryWords.length; i++) {
        var word = queryWords[i];
        querytfIdfsVec[i] = queryTfs[word] * queryIdfs[word];
    }
    urls.forEach(function(url) {
        var doctfIdfsVec = [];
        for (var i = 0; i < queryWords.length; i++) {
            var word = queryWords[i];
            var tfidfWordEntry = $.grep(tfidfData, function (e) {
                return e.word === word;
            });
            var tfidfList = tfidfWordEntry.length > 0 ? tfidfWordEntry[0].mapList : null;

            if (tfidfList !== null) {
                var tfidfObj = $.grep(tfidfList, function (e) {
                    return e.url === url;
                });
                var tfidf = tfidfObj.length > 0 ? tfidfObj[0]['tf-idf'] : 0;
            } else var tfidf = 0;
            doctfIdfsVec[i] = Math.abs(Number(tfidf));
        }
        console.log("querytfIdfsVec: ", isOneTermQuery(querytfIdfsVec));
        if (isOneTermQuery(querytfIdfsVec)) cosineScores[url] = similarity(querytfIdfsVec, doctfIdfsVec);
        else cosineScores[url] = getOneTermVal(doctfIdfsVec);
    });
    return cosineScores;
};

/* rankPages runs the ranking algorithm on the tfidf scores
   and the page rank data. Returns a sorted list of pageRankScorepairs
 */
var rankPages = function(cosineScores, prData, urls) {
    var pageScorePairs = [];
    urls.forEach(function(url) {
        var prEntry = $.grep(prData, function (e) {
            return e.url === url;
        });
        var pageRank = prEntry.length > 0 ? prEntry[0]['rank'] : .00001;
        var cosineScore = cosineScores[url];

        var rankScore = cosineScore * pageRank;
        pageScorePairs.push({
            url: url,
            rankScore: rankScore,
            pageRankScore: pageRank,
            cosineScore: cosineScore
        })
    });
    var sortedPageRankPairs = pageScorePairs.sort(function (a,b) {
        if (a.rankScore > b.rankScore) return -1;
        else if (a.rankScore < b.rankScore) return 1;
        else return 0;
    });
    return sortedPageRankPairs;
};

var isDataValid = function(data) {
    if (data == null || data == undefined) return false;
    if (!data.hasOwnProperty('Responses')) return false;
    return true;
};

var removeDuplicates = function(names) {
    var uniqueNames = [];
    $.each(names, function(i, el){
        if($.inArray(el, uniqueNames) === -1) uniqueNames.push(el);
    });
    return uniqueNames;
};


var parseDBData = function(data, tableName) {
    var args = Array.prototype.slice.call(data);
    var tfidfDataArr = Array.prototype.slice.call(args);
    var tfidfData = [];
    tfidfDataArr.forEach(function(tfidfObj) {
        if (tfidfObj.err || !isDataValid(tfidfObj.data)) {
            return null;
        }
        else tfidfData = tfidfData.concat(tfidfObj.data.Responses[tableName]);
    });
    return tfidfData;
};

var getSearchResults = function(query, deffered) {
    var queryWords = query.split(" ");
    db.getAllPageTfIdfs(queryWords).done(function() {
        var tfidfData = parseDBData(arguments, db.TFIDF_TABLE_NAME);
        if (tfidfData == null) {
            deffered.reject();
            return;
        }
        var urls = [];
        tfidfData.forEach(function (tfidfObj){
            urls = urls.concat(getUrlList(tfidfObj));
        });
        urls = removeDuplicates(urls);

        var queryTfs = calcTermFrequencyScore(queryWords);
        console.log(queryTfs);
        var queryIdfs = calcQueryIdf(queryWords, tfidfData, urls);
        console.log(queryIdfs);
        var cosineScores = calculateCosineSimilarity(queryWords, queryTfs, queryIdfs, tfidfData, urls);
        db.getAllPageRanks(urls).done(function() {
            var prData = parseDBData(arguments, db.PAGE_RANK_TABLE_NAME);
            if (prData == null) {
                deffered.reject();
                return;
            }
            var rankedPages = rankPages(cosineScores, prData, urls);
            db.getAllPageTitles(urls).done(function () {
                var titleData = parseDBData(arguments, db.TITLES_TABLE_NAME);

                if (titleData !== null || titleData !== undefined) {
                    for (var i = 0; i < rankedPages.length; i++) {
                        var titleObj = $.grep(titleData, function (e) {
                            return e.url === rankedPages[i].url;
                        });
                        if (titleObj.length > 0) rankedPages[i].title = titleObj[0].title;
                    }
                }
                deffered.resolve(rankedPages);
            });
        });
    });
};

module.exports = router;

//var query = "The game of life is a game of everlasting learning";
//var queryWords = query.split(" ");
//calcTermFrequencyScore(queryWords);
//
//var query = "The unexamined life is not worth living";
//var queryWords = query.split(" ");
//calcTermFrequencyScore(queryWords);
//
//var query = "Never stop learning";
//var queryWords = query.split(" ");
//calcTermFrequencyScore(queryWords);