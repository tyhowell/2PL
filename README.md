# 2PL Implementation using Java RMI, JDBC, and PostgreSQL
## Tyler Howell (howell66@purdue.edu) 
## Purdue CS54200 Spring 2020

1.	Launch postgresql servers on master and slave sites:
    -	bin/pg_ctl -D data/ restart
    -	(optional) bin/psql -d test -U remotereader
2.	Compile code if changes made:
    -	javac *.java
3.	RMI nonsense:
    -	(optional) rmic CentralSiteRemote
    -	rmiregistry 5000
4.	Start Central concurrency controller:
    -	java –cp /homes/howell66/Library/Java/Extensions/postgresql-42.2.10.jar:. MyServer
5.	Issue client requests
    -	java –cp /homes/howell66/Library/Java/Extensions/postgresql-42.2.10.jar:. RemoteSiteImpl 0 t1
    -   java –cp /homes/howell66/Library/Java/Extensions/postgresql-42.2.10.jar:. RemoteSiteImpl 1 t1

## Metrics
- [ ] Consider at least four sites, each site with a copy of the database. Assume a fully replicated database.
- [x] Use a non-distibuted database on each site, such as SQLite, PostgreSQL.
- [x] Use a centralized control for global decisions with a Central Site. The lock table has to be store at the Central Site.
- [x] All transactions arriving at different sites are sent to the Central Site. Queues for requesting and releasing locks will be maintained at the Central Site.
- [x] Processing must take place at the site where the transaction is submitted. Locks will be released after waiting on the database at the centralized site. Updates for other sites will be sent and they may arrive in a different order.
- [x] You may implement the algorithm of section 11.3.1 of our text book (Ozsu) or a variation of it.
- [ ] The 2PL implementation must ensure that all updates at all sites are posted in the same order. Furthermore, you must detect/resolve deadlocks. Please refer to section 11.6 in the textbook.

## To Do
- [ ] FIGURE OUT WHY t2 doesn't work
- [ ] Implement finer grained lock granularity
- [ ] Queueing for locks is not correctly implemented
- [ ] Lock managers should check if lock already held by transaction
- [ ] Detect deadlocks (especially if upgrading ones own lock from read to write)

## To Test
- [ ] Lock manager lock already held by same tid
- [ ] Transactions, releaseAllLocks, Commit, rollback
## Notes
- PostgreSQL DB: centralsite, remotesite0, remotesite1, remotesite2, remotesite3