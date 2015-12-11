/**
 * Created by adamcole on 12/8/15.
 *//**
 * Created by adamcole on 12/7/15.
 */
/**
 * routes autocomplete ajax requests to dictionary module and returns result
 */

var express = require('express');
var router = express.Router();
var dictionary = require("../data/dictionary");


/* GET return autocorrect */
router.get('/', function(req, res, next) {
    var prefix = req.query.prefix;
    var suggestedWords = dictionary.suggestWords(prefix, 10);
    res.send(suggestedWords);
});

module.exports = router;

