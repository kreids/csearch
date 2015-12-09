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
    var query = req.query.searchQuery.toLowerCase();
    var deferred = $.Deferred();
    deferred.done(function (rankedPages) {
        res.send(rankedPages);
    }).fail(function(err) {
        res.send(query + " failed");
    });
    getSearchResults(query, deferred);
});

/* getUrlList returns the urls in the tfidfObj */
var getUrlList = function(tfidfObj) {
    var urls = [];
    tfidfObj.mapList.forEach(function (urlScorePair) {
       urls.push(urlScorePair.url);
    });
    return urls;
};


var calculateCosineSimilarity = function(query, tfidfData) {

}

/* rankPages runs the ranking algorithm on the tfidf scores
   and the page rank data. Returns a sorted list of pageRankScorepairs
 */
var rankPages = function(tfidfData, prData) {
    var pageScorePairs = [];
    prData.forEach(function(prObj) {
        var tfEntry = $.grep(tfidfData[0].mapList, function (e) {
            return e.url === prObj.url;
        });
        var rankScore = tfEntry[0]['tf-idf'] * prObj.rank;
        pageScorePairs.push({
            url: prObj.url,
            rankScore: rankScore
        })
    });

    var sortedPageRankPairs = pageScorePairs.sort(function (a,b) {
        if (a.rankScore > b.rankScore) return -1;
        else if (a.rankScore > b.rankScore) return 1;
        else return 0;
    });
    return sortedPageRankPairs;
};

/* getSearchResults queries the db to get the data to
   generate a list of ranked search results
 */
var getSearchResults = function(query, deffered) {
    var queryWords = query.split(" ");
    db.getTfIdfs(queryWords, function(err, data) {
        var tfidfData = data.Responses[db.TFIDF_TABLE_NAME];
        var urls = [];
        tfidfData.forEach(function (tfidfObj){
            urls = urls.concat(getUrlList(tfidfObj));
        });

        var cosineScores = calculateCosineSimilarity(query, tfidfData);

        db.getPageRanks(urls, function(err, data) {
            var prData = data.Responses[db.PAGE_RANK_TABLE_NAME];
            var rankedPages = rankPages(tfidfData, prData);
            console.log(urls);
            db.getPageTitles(urls, function (err, data) {
                var titleData = data.Responses[db.TITLES_TABLE_NAME];
                console.log(titleData);
                rankedPages.forEach(function (page) {
                    console.log(page);
                    var titleObj = $.grep(titleData, function (e) {
                        console.log(e.url === page.url);
                        return e.url === page.url;
                    });
                    if (titleObj.length > 0) page.title = titleObj[0].title;
                });

                deffered.resolve(rankedPages);
            });
        });
    });
};
module.exports = router;
