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
    return urls;
};


var getWordFrequency = function(word, arr) {
    var count = 0;
    arr.forEach(function(arrVal) {
       if (word === arrVal) count++;
    });
    return count;
};

var calcTermFrequencyScore = function(queryWords) {
    console.log(queryWords);
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
        else var idfScore = 1 + Math.log(urls.length / docCount);
        idfScores[word] = idfScore;
    });
    return idfScores;
};

var docProduct = function(a,b) {
    var n = 0;
    var lim = Math.min(a.length, b.length);
    for (var i = 0; i < lim; i++) n += a[i] * b[i];
    return n;
};

function norm(a) {
    var sumsqr = 0;
    for (var i = 0; i < a.length; i++) sumsqr += a[i]*a[i];
    return Math.sqrt(sumsqr);
};

var similarity = function(a, b) {
    return docProduct(a,b) / (norm(a) * norm(b));
}

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
            //console.log("wordEntry: " + tfidfWordEntry);
            var tfidfList = tfidfWordEntry.length > 0 ? tfidfWordEntry[0].mapList : null;
            //console.log(tfidfList);

            if (tfidfList !== null) {
                var tfidfObj = $.grep(tfidfList, function (e) {
                    return e.url === url;
                });
                var tfidf = tfidfObj.length > 0 ? tfidfObj[0]['tf-idf'] : 0;
            } else var tfidf = 0;
            doctfIdfsVec[i] = Number(tfidf);
        }
        console.log(querytfIdfsVec, doctfIdfsVec);
        cosineScores[url] = similarity(querytfIdfsVec, doctfIdfsVec);
    });
    console.log(cosineScores);
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
        var pageRank = prEntry.length > 0 ? prEntry[0]['rank'] : 0;
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

/* getSearchResults queries the db to get the data to
   generate a list of ranked search results
 */
var getSearchResults = function(query, deffered) {
    var queryWords = query.split(" ");
    db.getTfIdfs(queryWords, function(err, data) {
        if (err || !isDataValid(data)) {
            deffered.reject();
            return;
        }
        var tfidfData = data.Responses[db.TFIDF_TABLE_NAME];
        var urls = [];
        tfidfData.forEach(function (tfidfObj){
            urls = urls.concat(getUrlList(tfidfObj));
        });
        urls = removeDuplicates(urls);

        var queryTfs = calcTermFrequencyScore(queryWords);
        var queryIdfs = calcQueryIdf(queryWords, tfidfData, urls);
        var cosineScores = calculateCosineSimilarity(queryWords, queryTfs, queryIdfs, tfidfData, urls);

        db.getPageRanks(urls, function(err, data) {
            if (err || !isDataValid(data)) {
                deffered.reject();
                return;
            }
            var prData = data.Responses[db.PAGE_RANK_TABLE_NAME];
            var rankedPages = rankPages(cosineScores, prData, urls);
            db.getPageTitles(urls, function (err, data) {
                if (err || !isDataValid(data)) {
                    deffered.reject();
                    return;
                }
                var titleData = data.Responses[db.TITLES_TABLE_NAME];
                for (var i = 0; i < rankedPages.length; i++) {
                    var titleObj = $.grep(titleData, function (e) {
                        return e.url === rankedPages[i].url;
                    });
                    if (titleObj.length > 0) rankedPages[i].title = titleObj[0].title;
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