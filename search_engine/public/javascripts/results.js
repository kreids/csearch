/**
 * Created by adamcole on 12/7/15.
 */
/**
 * Created by adamcole on 12/7/15.
 */
app = (function() {
    SEARCH_SUBMIT_ID = "#search-submit";
    SEARCH_INPUT_ID = "#search-input";
    RESULTS_CONTAINER_ID = "#results-container";


    var getQuery = function() {
        var query = $(SEARCH_INPUT_ID).val();
        if (query == null || query.trim() === "") return null;
        else return query;
    }

    var getSearchResults = function() {
        var query = getQuery();
        if (query == null) return;
        console.log(query);
        var getRequestUrl = "/search/?searchQuery=" + encodeURIComponent(query);
        var searchResults = $.get(getRequestUrl)
            .done(function(data) {
                console.log("search results: " + data);
                populateSearchResults(data);
             })
            .fail(function(err) {
                console.log("error getting search results");
            })
            .always(function() {
                console.log("always after getting results");
            });
    }

    var populateSearchResults = function(results) {
        $(RESULTS_CONTAINER_ID).append("<h1>added search results:" + results + "</h1>")
    };

    var searchSubmitHandler = function() {
        var query = getQuery();
        if (query != null) window.location.replace("/results?searchQuery=" + encodeURIComponent(query));
    }

    var attachEventHandlers = function() {
        $(SEARCH_SUBMIT_ID).click(searchSubmitHandler);
    }

    return {
        initialize: function () {
            console.log("results initialized");
            attachEventHandlers();
            getSearchResults();
        }
    }

}());

$(function() {
    app.initialize();
});