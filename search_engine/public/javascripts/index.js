/**
 * Created by adamcole on 12/7/15.
 */
app = (function() {
    SEARCH_SUBMIT_ID = "#search-submit";
    SEARCH_INPUT_ID = "#search-input";

    var searchSubmitHandler = function() {
        console.log("search submitted");
        var query = $(SEARCH_INPUT_ID).val();
        if (query == null || query.trim() === "") return;
        else {
            window.location.replace("/results?searchQuery=" + encodeURIComponent(query));
        }
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
            console.log("app initialized");
            attachEventHandlers();
        }
    }

}());

$(function() {
   app.initialize();
});