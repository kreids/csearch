/**
 * Created by adamcole on 12/7/15.
 */
var express = require('express');
var router = express.Router();
var db = require('../db/db');


/* GET search results. */
router.get('/', function(req, res, next) {
    var query = req.query.searchQuery;
    res.send(query);
});


// get ididf value to process
// calculate cosine simmilarity
// get page rank

//console.log(db.getPageRanks(["j","i","h"]));
console.log(db.getPageRanks(["j","i","h"], function(err, data) {
    console.log(data);
}));

console.log(db.getTfIdfs(["hello"], function(err, data) {
    console.log(data);
}));

module.exports = router;
