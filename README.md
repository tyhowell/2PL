# 2PL Implementation using Java RMI, JDBC, and PostgreSQL
## Tyler Howell (howell66@purdue.edu) 
## Purdue CS54200 Spring 2020

1.	Launch postgresql servers on master and slave sites:
    -	bin/pg_ctl -D data/ restart
    -	(optional) bin/psql -d remotesite0 -U remotereader
    -   turn autocommit off (\set AUTOCOMMIT off)
2.	Compile code if changes made:
    -	javac *.java
3.	RMI nonsense:
    -	(optional) rmic CentralSiteRemote
    -	rmiregistry 5000
4.	Start Central concurrency controller:
    -	java -cp postgresql-42.2.10.jar:. MyServer
5.	Issue client requests
    -   java -cp postgresql-42.2.10.jar:. RemoteSiteImpl 0 t1
    -	java -cp postgresql-42.2.10.jar:. RemoteSiteImpl 1 t1

## File Directory
├── _README.md
├── _test
|   ├── t1
|   ├── t2
|   └── t3
├── _CentralSite.java
├── _CentralSiteImpl.java
├── _ConcurrentLockNode.java
├── _GlobalWaitForGraph.java
├── _Lock.java
├──_MyServer.java
├──_Operation.java
├──_RemoteSite.java
└── _RemoteSiteImpl.java

## Metrics
- [x] Consider at least four sites, each site with a copy of the database. Assume a fully replicated database.
- [x] Use a non-distibuted database on each site, such as SQLite, PostgreSQL.
- [x] Use a centralized control for global decisions with a Central Site. The lock table has to be store at the Central Site.
- [x] All transactions arriving at different sites are sent to the Central Site. Queues for requesting and releasing locks will be maintained at the Central Site.
- [x] Processing must take place at the site where the transaction is submitted. Locks will be released after waiting on the database at the centralized site. Updates for other sites will be sent and they may arrive in a different order.
- [x] You may implement the algorithm of section 11.3.1 of our text book (Ozsu) or a variation of it.
- [x] The 2PL implementation must ensure that all updates at all sites are posted in the same order. Furthermore, you must detect/resolve deadlocks. Please refer to section 11.6 in the textbook.

## To Do
- [ ] Implement finer grained lock granularity
- [ ] Support flexible/new tables
- [ ] Support multi-table queries (or add to a limitations page in presentation)
- [ ] Complete all TODOs
- [ ] Remove all unnecessary comments, printlns, 

## To Test
- [ ] Lock manager lock already held by same tid
- [ ] More rigorous deadlock test cases
- [ ] Delete

## Notes
- PostgreSQL DB: centralsite, remotesite0, remotesite1, remotesite2, remotesite3
## Test DB schema
Table: student
 id |  name | username | grade 
Table: job
 job_id | job_name | salary | department
