Hagrid
======

## What is Hagrid:

Hagrid is a tool to fetch, store, and visualize job level counters for each Hadoop job. When a Hadoop job is run the JobTracker stores a large amount of information about the job, but it only stores it for a short period of time. Hagrid is capable of fetching this information and storing it in a MySQL database. Hagrid also provides a UI where users can enter in the Job ID of their Hadoop job and Hagrid will return a series of graphs and tables to display all the counters. Example of the counters include task time, spilled records, combiner input / output, etc. The true benefit of Hagrid lies in the advice section that is present on each page that Hagrid returns. Hagrid is capable of doing computation over all the data and returning advice to user on how to improve and debug their Hadoop job.

## How to Configure and Start Hagrid:

* Setup a MySQL database that the application can store logs in
* Place the config XML files for the Hadoop cluster into the src folder
* Edit the properties file located in WebContent/META-INF and add in the database name, user, and password
* Export the project as a WAR and place the WAR in the webapps folder of a Tomcat or Jetty instance
* The application can now be accessed through the link HostName/Hagrid/HomePage.jsp

## How to Use Hagrid:

### Starting Hagrid

Once Hagrid has been properly configured, the java class LogFetchJobTracker.java needs to be run a server. It is setup to repeat execution every 15 minutes so it is important to leave the class running.

### Getting the Graphs

Once the data is in the MySQL server, any of the jobs that have been processed can be visualized. A user only needs to input the Job Id into the form and press "Submit". An example Job ID would be: job_000000000000_000000

## How to Contribute:

The true potential of Hagrid lies in the advice section of the tool. Under the rules package anyone can add a rule that does some sort of computation over the data and returns advice to the user. As the project continues to grow the advice will hopefully be more and more helpful.