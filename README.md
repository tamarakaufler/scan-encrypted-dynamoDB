# scan-encrypted-dynamoDB
Standalone Java tool to do a search of an AWS dynamoDB table containing encrypted data

* fetches data from loyalty-service DynamoDB table (decrypts on the fly)
* creates a csv file with ids of customers who added their loyalty cards to their account within a particular period of time:

```
Customer ID,Loyalty Card Number,Added Card Date

1,25497611111,2017/02/15
22,92778692222,2017/02/24
36,71390233333,2017/03/12
59,56815455555,2017/02/15
666,72166666666,2017/02/16
...
```

* card is in plain text
* current search interval: 017-01-01T00:00:00 till 2027-12-24T00:00:00; ie effective since the release of the loyalty-service till the run of the script

Prerequisites
=============
1) before running, make sure ~/.aws/credentials contains AWS credential for the correct account
2) the application entry point: AddedCardsFile.java 
3) compile and run (gradle, maven) 

Outcome
=======

creates file    Added Loyalty Cards yyyMMddHHmmss.csv in the dir where the tool is run

