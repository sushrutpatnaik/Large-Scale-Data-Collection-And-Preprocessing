# Large Scale Data Collection and preprocessing in Spark

As part of a political event coding pipeline, we are going to design a spark based
preprocessing tool that does the following steps
1. Collects data by crawling a set of spanish news websites on a daily basis.
2. Extracts the main content of the article and related metadata (i.e headline, author, date
published)
3. Process the extracted content with udpipe and generate universal dependency parse for
each sentences within the content (use Apache Spark here)
4. Store the collected data and processed data in a way compatible with event coder’s
input format in MongoDB
5. Running some deduplication algorithm at content level. (Comparing two articles from
different urls and find out whether they cover the same story)