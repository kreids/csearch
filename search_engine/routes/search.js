/**
 * Created by adamcole on 12/7/15.
 */
var express = require('express');
var router = express.Router();

/* GET search results. */
router.get('/', function(req, res, next) {
    var query = req.query.searchQuery;
    res.send(query);
});

module.exports = router;
