1. How to run

hadoop jar /home/yuz1988/lab5/exp1/Exp1.jar Exp1 -libjars /home/yuz1988/lab5/jsonsimple1.1.1.jar /class/s14419x/lab5/yolo.json /scr/yuz1988/lab5/exp1/output


I use the jsonsimple1.1.1.jar and put it in "/home/yuz1988/lab5/jsonsimple1.1.1.jar".
The first argument "/class/s14419x/lab5/yolo.json" is the input,
the second argument "/scr/yuz1988/lab5/exp1/output" is the output.
Also the temp folder I used is "/scr/yuz1988/lab5/exp1/temp" which is hardcoded in my program.
So clear the above folders by "hadoop fs -rmr /scr/yuz1988/lab5/exp1".


2. Map-Reduce Algorithm

MR round 1:
find the hashtag and group the tweets by hashtags, in the reduce process, count the number of tweets for each hashtag.

MR round 2:
map all the input to one reducer and sort all the records by the count. Output the ten most common hashtags.
