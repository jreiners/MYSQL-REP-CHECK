#!/usr/bin/env python
#
# filename = rep_stat.py
#
# check replication status

import sys,MySQLdb,time
tprint = sys.stdout.write

# set directory and filename for logging
logdir = "/var/log/rep_stat/"
logfilename = "rep_stat.log"

# rep_logger function - writes log to file
def rep_logger(message):
    logfile = open ( "%s%s" % (logdir,logfilename) , "a" )
    logfile.write ( ("%s:    %s%s") % (time.ctime(),message,"\n" ) )
    logfile.close()

### get mysql root password

# where to find password
mycnf="/root/.my.cnf"

# simple file parser - can be used in a more general sense as well
def getpassword():
    try:
        mycnfopen = open( mycnf , "r" )
    except:
        rep_logger ( "could not find my.cnf" )
        sys.exit()
    mycnflines = mycnfopen.readlines()
    for line in mycnflines:
        if line.find("password") > -1:
            splitline = line.split("=")
            password = splitline[1] 
            # here is a little hack used to make the newline character disappear
            if password[-1:] == "\n":
                password = password[:-1]
            return password

# connect to MySQL

# connection details
db_host_master = "localhost"
db_host_slave = "10.0.170.20"
db_user = "root"
db_passwd = getpassword()
db_dbname = "status_example" 

# utc is good to prevent tz issues

# epochnow returns seconds since epoch (disregard values smaller than a second)
def epochnow():
    return int(time.time())

# define some queries:
query_getlastcheck = "SELECT lastcheck FROM lastcheck"
query_updatelastcheck = ( "UPDATE lastcheck SET lastcheck = %s" % epochnow() )
query_slavestatus = ( "SHOW SLAVE STATUS" )

# do some logic
try:
    conn = MySQLdb.connect ( 
                            host = db_host_master,
                            user = db_user,
                            passwd = db_passwd,
                            db = db_dbname
                            ) 
    cursor = conn.cursor()
    cursor.execute (query_getlastcheck)
    row = cursor.fetchone()
    # leaving connection open for rest of script unless disaster
except:
    # here local mysql fails - return slave
    rep_logger ("Error connecting to master DB") 
    tprint ("200")
    sys.exit()

try:
    slaveconn = MySQLdb.connect (
                                host = db_host_slave,
                                user = db_user,
                                passwd = db_passwd,
                                db = db_dbname
                                )
    curslave = slaveconn.cursor()
    curslave.execute (query_getlastcheck)
    rowslave = curslave.fetchone()
    curslave.close()
    slaveconn.close()
except:
    # here slave mysql fails - return master
    rep_logger ("Error doing query on slave DB")
    tprint ("100")
    sys.exit()


timedelta = (epochnow() - row[0])

if ( timedelta <= 20 ):
    # normal - things are ok
    rep_logger ( "THIS IS A TEST - EVERYTHING IS GOOD" ) # for testing
    cursor.execute (query_updatelastcheck)
    cursor.close()
    conn.close()
    tprint ("100")
    sys.exit()
elif ( timedelta >20 ):
    # apparently we missed a check for some reason
    # do more testing for status
    rep_logger ( "LastCheck > 20 seconds" )
    # seconds behind (last in show slave status tuple, hence the '-1')
    cursor.execute (query_slavestatus)
    row = cursor.fetchone()
    try:
        # hopefully we get an int back
        seconds_behind = int(row[-1])
    except:
        # something strange has happened, leave the master alone
        rep_logger ( "Seconds behind not integer" )
        cursor.close()
        conn.close()
        tprint ("200")
        sys.exit()
    if ( seconds_behind == 1 or seconds_behind == 0 ):
        # mysql failed but came back and is replicating normally
        rep_logger ( "Seconds behind <=1 is good..." ) # for testing
        cursor.execute (query_updatelastcheck)
        cursor.close()
        conn.close()
        tprint ("100")
        sys.exit()
    elif ( seconds_behind > 1 ):
        # mysql failed, but came back ok, but is still behind in replication
        # maybe next check it will be ok
        rep_logger ( "Seconds behind >1 still busy catching up, come back later" ) # for testing
        cursor.close()
        conn.close()
        tprint ("200")
        sys.exit()
    else:
        # something strange has happened, leave the master alone
        rep_logger ( "Seconds behind invalid value" )
        cursor.close()
        conn.close()
        tprint ("200")
        sys.exit()

else: # final timedelta case
    # something here went wrong, log and return slave
    rep_logger ( "TIMEDELTA FATAL ERROR" )
    cursor.close()
    conn.close()
    tprint ("200")
    sys.exit()
