kreids <- me
rohanb
adamcole
awiss

This project indexes words to a list of maps that map the string "url" to a url that contains the word and the string "if-idf" to the tf-idf value for that word on that url

source files
	Downloader
	idfDriver
	idfMapReduce
	prelimDriver
	prelimMapReduce
	tablePopulator

1.To run this file first run input populator to get the input
2.Next run the prelim driver with hadoop 2.7.1
3.Next run the idf driver with hadoop 2.7.1
4.Then count the lines of the file produced by using vim and typing in G to skip to the end of the file. Divide that number by the number of threads you wish to run and type the command split -l (number from division) out/out1/partr-00000 seg to split the file into multiple output files.
5.Then as many instances of table populator as you have segfile run table populator with arg0=segXX arg1= number of files in corpus.



