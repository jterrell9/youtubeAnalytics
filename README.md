# youtubeAnalytics
This application is a suite of spark functions designed to produce reduced datasets of a youtube dataset of trending videos.  The various functions are applied to a selected set of categories.  The menu offers 14 selections, each representing a different spark function.
MAIN MENU:
1: Choose Categories
2: Count Videos by Categories
3: Filter Videos by Categories
4: Videos with Top K Likes
5: Videos with Top K Dislikes
6: Videos with Top K Views separated by category
7: Videos with Top K Views of all categories
8: Videos with Top K Likes of all categories
9: Videos with Top K Dislikes of all categories
10: Videos with Top K views of all categories of videos from 2018
11: Videos with Top K likes of all categories of videos from 2018
12: Videos with Top K dislikes of all categories of videos from 2018
13: View Selected Categories
14: Quit

This application requires a Hadoop cluster, and the "USvideos.csv" file must be partitioned on a HDFS enabled datanode cluster.

To run the application, simply run the shell script called "run" in the parent directory.
