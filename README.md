# Similar-Users-using-LSH
Locality Sensitive Hashing in Python Spark to find similar users

Solving the problem of using LSH to find similar users, based on the fraction of the movies they have watched in common.

## A. Problem
Suppose there are 100 different movies, numbered from 0 to 99. A user is represented as a set of movies. Jaccard coefficient is used to measure the similarity of sets.

## B. Workaround
<li> Using minhash to obtain a signature of 20 values for each user. Recall that this is done by "logically" permuting the rows of characteristic matrix of movie-user matrix (i.e., row are movies and columns represent users). </li>
<li> The i-th hash function for the signature: h(x,i) = (3x + 13i) % 100, where x is the original row number in the matrix. </li>
<li> LSH is used to speed up the process of finding similar users, where the signature is divided into 5 bands, with 4 values in each band. </li>
<li> Finding similar users: Based on the LSH result, for each user U, top-5 users who are most similar to U are reported. </li>

## Input format:
Input file with each line representing a user (with ID “U1” for example) and a list of movies the user has watched.
U1,0,12,45
U2,2,3,5,99
…

## Output format:
Top-5 similar users as:
U2:U9,U10
U3:U8
U8:U3
…

## Performance:
Multiple divisions of data run parallely, for a huge dataset. It takes nearly 10 seconds to compute for a batch of 30000 users.
