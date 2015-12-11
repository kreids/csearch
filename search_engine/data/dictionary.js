var fs = require('fs');
var dictionary = [];

/**
 * dictionary loads in a dictionary of common english words
 */

fs.readFile('data/wiktionary.txt', 'utf8', function (err, data) {
    if (err) {
        return console.log(err);
    }

    var lines = data.split(/[ ,\t\n]+/)
    for (var i = 0; i < lines.length; i++) {
        var code = lines[i].charCodeAt(0);
        if ( ((code >= 65) && (code <= 90)) || ((code >= 97) && (code <= 122)) ) {
            dictionary.push(lines[i]);
        }
    }
});


exports.suggestWords = function(prefix, n) {
    var suggestions = [];
    for (var i = 0; i < dictionary.length; i++) {
        if (dictionary[i].startsWith(prefix)) suggestions.push(dictionary[i]);
        if (suggestions.length >= n) return suggestions;
    }
    return suggestions;
}