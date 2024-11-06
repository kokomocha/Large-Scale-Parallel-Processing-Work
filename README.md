<p align="center">  
    <br>
	<a href="#">
	      <img src="https://cdn.svgporn.com/logos/java.svg" alt="Java" title="Java" width ="120" />
        <img height=100 src="https://cdn.svgporn.com/logos/scala.svg" alt="Scala" title="Scala" hspace=20 /> 
        <img height=100 src="https://cdn.svgporn.com/logos/intellij-idea.svg" alt="IntelliJ" title="IntelliJ" hspace=20 /> 
        <img height=100 src="https://cdn.svgporn.com/logos/hadoop.svg" alt="Hadoop" title="Hadoop" hspace=20 />
        <img height=100 src="https://cdn.svgporn.com/logos/apache-spark.svg" alt="Spark" title="Spark" hspace=20 /> 
        <img height=100 src="https://cdn.svgporn.com/logos/maven.svg" alt="Maven" title="Maven" hspace=20 /> 
  </a>	
</p>
<br>


# Large Scale Parallel Data Processing
The algorithms in this repository are designed to work with big data (~2 TB or more). 

## Applications
The repository contains codework for Large Scale Parallel Data Network Analyses for:
1. Page Rank Algorithm:
- The Page Rank Algorithm is highly applicable in analyzing chain of popular websites or Social network pages.
- It is useful in any adjacency graph problem involving Markov Chains with static probabilities.

2. Triangle Count Algorithm:
- The Triangle Count Algorithm is highly applicable to analyze common subsets of Social Network groups. It further lays foundations to analyze group behaviors, find similarities, and build recommendations.
- Another interesting application is for finding 2 hop destinations for connecting flights or supply chain delivery networks.
- This algorithm is the basis of more complicated analyses beyond 3 entities(people or destinations).

## Design Patterns Utilized
1. Hash-Shuffle Joins
2. Partition-Broadcast Joins
3. Custom Partitioners
4. Combined Key Comparators for fine granularity
5. Dummy Variable for 1-pass aggregations
6. M-Bucket Randomized Operations

## Frameworks and Tools
The algorithms were written for Distributed Frameworks:
1. MapReduce using Java SE 11
2. Spark using Scala 3

The data sets for each of the problems contained 10M+ rows and I successfully tested them on AWS-EMR clusters.
The tests were carried for different config combinations of:
1. Cluster type (m4, m5, m6, m4a, m5a, m6a)
2. Number of nodes (1, 2, 4, 8)
3. Dataset sub-sizes

I also configured the setup for on local machine using the pseudo-cluster mode in Hadoop.
Other tools used: IntelliJ, Hadoop(Hdfs), Maven, Yarn

## Results
I achieved superior performance, accomplishing 10x speedability (reducing processing times from 100 minutes to 10 mins) and approximately linear scalability.
