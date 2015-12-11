Adam Cole

# CSEARCH SEARCH ENGINE AND UI

The search engine is implemented in Javascript using Node. This module provides
a search input which gets sent the backend. The server processes the query,
gets the relevant information from the Amazon DynamoDB servers, ranks the pages
according a ranking algorithm and outputs the results back to the client. 

This module also includes autocomplete on the home page and "Did you mean:" spell checking.

## How to run locally
1. Make sure node is downloaded (if it's not, install it)
2. Run 'npm install'
3. Run 'npm start' 
4. Go to http://localhost:3000/ in your browser

(note: you need to set up amazon credentials to successfully run the program)