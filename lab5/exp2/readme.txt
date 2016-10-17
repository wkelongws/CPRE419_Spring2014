1. How to run

hadoop jar /home/yuz1988/lab5/exp2/Exp2.jar Exp2 -libjars /home/yuz1988/lab5/jsonsimple1.1.1.jar /class/s14419x/lab5/yolo.json /scr/yuz1988/lab5/exp2/output


I use the jsonsimple1.1.1.jar and put it in "/home/yuz1988/lab5/jsonsimple1.1.1.jar".
The first argument "/class/s14419x/lab5/yolo.json" is the input,
the second argument "/scr/yuz1988/lab5/exp2/output" is the output.
Also the temp folder I used is "/scr/yuz1988/lab5/exp2/temp" which is hardcoded in my program.
So clear the above folders by "hadoop fs -rmr /scr/yuz1988/lab5/exp2".


2. Map-Reduce Algorithm

MR round 1:
for each tweet, find the screen_name and follower_count, group by the screen_name. In the reduce phase, for each screen_name, find the largest follower_count

MR round 2:
map all the input to one reducer and sort all the records by the follower_count. Output the top ten most followed tweeters and their number of followers.