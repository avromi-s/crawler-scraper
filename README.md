# crawler-scraper

This program crawls the given (hardcoded) domain and then scrapes the downloaded pages to extract and compile useful pieces of data.

On start the program connects to or creates a NoSQL (MongoDB) database to keep track of which pages have been processed and to store all the info collected.

Every so often the program allows the user to display any or all of the different types of data so far collected by the program.
