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
    LOADING_VIEW_ID = "#loading-view";


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
                $(LOADING_VIEW_ID).hide();
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
        if (results.type === 'fail') $(RESULTS_CONTAINER_ID).append("<h3>No search results found</h3>")
        for (var i = 0; i < results.length; i++) {
            $(RESULTS_CONTAINER_ID).append("<div id=\"result-continer\">");
            if (results[i].hasOwnProperty('title')) $(RESULTS_CONTAINER_ID).append("<h3>" + results[i].title + "</h3>");
            else $(RESULTS_CONTAINER_ID).append("<h3>" + results[i].url + "</h3>");
            $(RESULTS_CONTAINER_ID).append("<a href=\"" + results[i].url + "\">" + results[i].url + "</a>");
            $(RESULTS_CONTAINER_ID).append("<p> totalScore: " + results[i].rankScore +
                "<br/> pageRankScore: " + results[i].pageRankScore +
                "<br/> cosineScore: " + results[i].cosineScore +
                "</p>");
            $(RESULTS_CONTAINER_ID).append("</div>");
        };
    };

    var searchSubmitHandler = function() {
        var query = getQuery();
        if (query != null) window.location.replace("/results?searchQuery=" + encodeURIComponent(query));
    }

    var keypressHandler = function(e) {
        if(e.which == 13) {
            searchSubmitHandler();
        }
    };

    var attachEventHandlers = function() {
        $(SEARCH_SUBMIT_ID).click(searchSubmitHandler);
        $(SEARCH_INPUT_ID).keypress(keypressHandler);
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