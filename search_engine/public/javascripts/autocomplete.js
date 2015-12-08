/**
 * Created by adamcole on 12/8/15.
 */
autocomplete = (function() {
    SEARCH_INPUT_ID = "#search-input";
    AUTOCOMPLETE_CONTAINER_ID = "#autocomplete-container";
    AUTOCOMPLETE_LIST_ITEM_SELECTOR = "#autocomplete-container li"

    var getQuery = function() {
        var query = $(SEARCH_INPUT_ID).val();
        if (query == null || query.trim() === "") return null;
        else return query;
    }

    var keyupHandler = function() {
        var query = getQuery();
        if (query == null) {
            hideAutoCompleteData();
            return;
        }
        var querySplit = query.split(" ");
        var prefix = querySplit[querySplit.length-1];
        if (prefix.trim() === "") {
            hideAutoCompleteData();
            return;
        }
        var getAutocompleteUrl = "/autocomplete?prefix=" + encodeURIComponent(prefix);
        $.get(getAutocompleteUrl).done(function(data) {
            console.log(data)
            showAutoCompleteData(data);
        }).fail(function(err) {
            hideAutoCompleteData();
            console.log("error getting search results");
        })
    };

    var showAutoCompleteData = function(data) {
        var html = "<ul>";
        $.each(data, function(index, word) {
           html = html + "<li>" + word + "</li>";
        });
        var html = html + "<ul>";
        console.log(html);
        console.log($(AUTOCOMPLETE_CONTAINER_ID));
        $(AUTOCOMPLETE_CONTAINER_ID).html(html);
        $(AUTOCOMPLETE_LIST_ITEM_SELECTOR).click(autocompletListClickHandler);
    }

    var hideAutoCompleteData = function() {
        $(AUTOCOMPLETE_CONTAINER_ID).html("");
    };

    var autocompletListClickHandler = function(e) {
        var selectedWord = $(e.target)[0].innerText;
        var query = getQuery();
        var newQuery = query.substring(0,query.lastIndexOf(" "));
        if (newQuery.trim() === "") newQuery = selectedWord;
        else newQuery = newQuery + " " +selectedWord + " ";
        $(SEARCH_INPUT_ID).val(newQuery);
        $(SEARCH_INPUT_ID).focus();
        hideAutoCompleteData();
    }

    return {
        initialize: function() {
            $(SEARCH_INPUT_ID).keyup(keyupHandler);
        }
    };
})();


$(function() {
    autocomplete.initialize();
});