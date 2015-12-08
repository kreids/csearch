var express = require('express');
var router = express.Router();


/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});


/* GET search page. */
router.get('/results', function(req, res, next) {
    // get results
    res.render('results', { searchQuery: req.query.searchQuery});
});

module.exports = router;
