# AWS-EMR-Collocation-Extraction

General description:
A collocation extraction that runs on Amazon EMR platform.
The program built wo run on 2-words bigrams.

How to compile:
1. compile "multiStep" to create the JAR with the hadoop running logic, put in s3.
2. compile "Main" to create the jar that setup the emr.
3. run localy the jar from step 2.

SYNOPSIS:
java -jar main-class A B

A: minimal pmi

B: relative minimal pmi

example: java -jar extractCollations.jar ExtractCollations 0.5 0.2


* java 1.8 or above must be installed
* data input and output are defined in Main.class
* .pem key for easy ssh connection to aws instances is not provided with this project
* .aws folder must be available with the proper configuration (credentials and region)
* aws configuration such as security profile and IAM role must be configured as well.
________________

Program flow:
job0:
- combine bigrams of the same decade into the same key-value pair. {(1975, w1, w2, 1), (1977, w1, w2, 2)} => (1970, w1, w2, 3)
- remove pairs with stop words.

job1:
- for each bigram extract C(w1).
- calculate the number of bigrams for each decade.

job2:
- for each bigram extract C(w2).
- calculate the npmi of each bigram.
- calculate the total values of npmi for each decade.

job3:
- filter the results according to the minimal and relative npmi given.
- sort the values by decade and npmi value.
