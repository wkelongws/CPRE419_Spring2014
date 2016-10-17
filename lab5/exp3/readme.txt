1. How to run

hadoop jar /home/yuz1988/lab5/exp3/Exp3.jar Exp3 -libjars /home/yuz1988/lab5/jsonsimple1.1.1.jar


I use the jsonsimple1.1.1.jar and put it in "/home/yuz1988/lab5/jsonsimple1.1.1.jar".
I hardcoded the input and output path (also the temp folder) in this program for my convenience. So there are no arguments for this program.
Clear the above folders by "hadoop fs -rmr /scr/yuz1988/lab5/exp3".


2. Map-Reduce Algorithm

MR round 1:
for each tweet, find the screen_name and all the hashtags in this tweet, group by the screen_name. In the reduce phase, for each screen_name, count the number of tweets he/she posted, also find the most common tag he/she used in all his/her tweets.

MR round 2:
map all the input to one reducer and sort all the records by the number of tweets posted for each tweeter. Output the top ten most prolific tweeters and their number of posts, most common hashtag and frequency of that tag.

3. Notice
For experiment 3, I think the output maybe a little bit different from the answer. First, for the hashtags, I didn't differentiate the lowercase and uppercase. Like "FIFA" and "fifa", I think they are two hashtags. Second, in my output, users "JonP9", "ZupFollowBack" and "ceoMARS" are having the same number of the posts, so the order maybe different. Third, some tags may have the same number of occurrences for some tweeters. For example, in the two copies of my outputs (first output just keeps the original tags, second is converting all the tags to lowercase), the most common tag for user "JonP9" may be "marsocial" or "CNN".