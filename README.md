# 2PL Implementation using Java RMI, JDBC, and PostgreSQL
## Tyler Howell (howell66@purdue.edu) 
## Purdue CS54200 Spring 2020

1.	Launch postgresql servers on master and slave sites:
a.	bin/pg_ctl -D data/ restart
b.	(optional) bin/psql -d test -U remotereader
2.	Compile code if changes made:
a.	javac *.java
3.	RMI nonsense:
a.	(optional) rmic CentralSiteRemote
b.	rmiregistry 5000
4.	Start Central concurrency controller:
a.	java –cp /homes/howell66/Library/Java/Extensions/postgresql-42.2.10.jar:. MyServer
5.	Issue client requests
a.	java –cp /homes/howell66/Library/Java/Extensions/postgresql-42.2.10.jar:. MyClientRemote
