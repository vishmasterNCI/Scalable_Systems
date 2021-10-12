# Scalable_Systems

This project aims to perform assosciation rule mining on a random set of tweets that I gathered related to COVID from twitter
The following algorithms were implemented:
1.Apriori
2.PCY
3.Multi-Stage PCY
4.Multi-Hash PCY

This project assumes you have pseudo distributed hadoop configuration and python installed.The requirements.txt will be available on request only.

Sample Output:
#coviddata,#smcanalytics,#covid,#coronavirus ---> #covid_ . support = 0.030216536443627626, confidence = 1.0
#covidvislualizations,#covid_ ,#coviddata,#covid ---> #coronavirus. support = 0.030216536443627626, confidence = 1.0
#covidvislualizations,#covid_ ,#coviddata,#coronavirus ---> #covid. support = 0.030216536443627626, confidence = 1.0
#covidvislualizations,#covid_ ,#covid,#coronavirus ---> #coviddata. support = 0.030216536443627626, confidence = 1.0
#covidvislualizations,#coviddata,#covid,#coronavirus ---> #covid_ . support = 0.030216536443627626, confidence = 1.0
#covid_ ,#coviddata,#covid,#coronavirus ---> #covidvislualizations. support = 0.030216536443627626, confidence = 1.0


The difference between Apriori and PCY algorithms can be understood by executing this code.

In most of the cases the PCY eliminates most of the rules.
