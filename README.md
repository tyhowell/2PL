# 2PL Implementation using Java RMI, JDBC, and PostgreSQL
### Tyler Howell (howell66 (at) purdue (dot) edu) 
### Purdue CS54200 Spring 2020

## Setup
1. [Install PostgreSQL](https://www.postgresql.org/docs/9.3/tutorial-install.html), 
2. Create 4 remote site databases (remotesite0, 1...) and one central site (centralsite) database
3. Create one user with all table privileges (name: remotereader, pw: bb)
## Launch
1. Start postgresql servers on central and remote sites:
    
        bin/pg_ctl -D data/ restart

        (optional) bin/psql -d remotesite0 -U remotereader
2.	Compile .java if first time or changes made:
        
        javac *.java
3.	Start RMI registry:
    
        (deprecated) rmic CentralSiteRemote

        rmiregistry 5000

4.	Start Central Site concurrency controller (cleanDB argument is optional):

        java -cp postgresql-42.2.10.jar:. MyServer cleanDB:=true

5.	Issue client requests (slowTime argument is optional):

        java -cp postgresql-42.2.10.jar:. RemoteSiteImpl 0 t1 slowTime:=true

        java -cp postgresql-42.2.10.jar:. RemoteSiteImpl 1 t1 slowTime:=true

    ...

## Project Requirements
- [x] Consider at least four sites, each site with a copy of the database. Assume a fully replicated database.
- [x] Use a non-distibuted database on each site, such as SQLite, PostgreSQL.
- [x] Use a centralized control for global decisions with a Central Site. The lock table has to be store at the Central Site.
- [x] All transactions arriving at different sites are sent to the Central Site. Queues for requesting and releasing locks will be maintained at the Central Site.
- [x] Processing must take place at the site where the transaction is submitted. Locks will be released after waiting on the database at the centralized site. Updates for other sites will be sent and they may arrive in a different order.
- [x] You may implement the algorithm of section 11.3.1 of our text book (Ozsu) or a variation of it.
- [x] The 2PL implementation must ensure that all updates at all sites are posted in the same order. Furthermore, you must detect/resolve deadlocks. Please refer to section 11.6 in the textbook.

## Future Work
- [ ] Implement finer grained lock granularity
- [ ] Support flexible/new tables
- [ ] Support multi-table queries (or add to a limitations page in presentation)

## Notes
- PostgreSQL DB: centralsite, remotesite0, remotesite1, remotesite2, remotesite3
- User: remotereader, Password: bb

### Test DB schema
student

 id |  name | username | grade 

job

 job_id | job_name | salary | department
