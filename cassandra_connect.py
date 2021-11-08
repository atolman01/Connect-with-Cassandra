################## Homework 3: Cassandra Inserts ##################

#####                   Amanda Tolman                         #####
#####                 ITEC 641 Fall 2021                      #####
#####                  October 3rd, 2021                      #####

###################################################################

###################################    HOW TO RUN THIS FILE     ################################### 
#####                                                                                         #####
#####  1. Download the cassandra_connect.py AND data_generator.py to your Downloads Directory #####
#####                                                                                         #####
#####  2. In the terminal, use the cd command to go to the Downloads Directory                #####
#####                                                                                         #####
#####      a. type the command: python3 cassandra_connect.py                                  #####
#####                                                                                         #####
#####      b. It will prompt to enter your Cassandra username and password                    #####
#####                                                                                         #####
#####      c. Insert the number of rows you wish to insert                                    #####
#####                                                                                         #####
#####      d. BOOM, rows are inserted into all four tables!                                   #####
#####                                                                                         #####
#####      e. Four CSVs will also be added to your Downloads Directory that contains data     #####
#####              for every table                                                            #####
#####                                                                                         #####
#####  3. SCP all four CSVs to RUCS                                                           #####
#####                                                                                         #####
#####  4. SSH to RUCS                                                                         #####
#####                                                                                         #####
#####      a. Go to Directory where you secure copied the four CSVs                           #####
#####                                                                                         #####
#####      b. Change permissions of the all files using chmod: chmod 744 filename.csv         #####
#####                                                                                         #####
#####      c. SCP all CSVs to ITEC-CASS07: scp username@itec-cass07                           #####
#####                                                                                         #####
#####  5. SSH to ITEC-CASS07: ssh username@itec-cass07                                        #####
#####                                                                                         #####
#####  6. Go into the Cassandra shell: cqlsh -u username                                      #####
#####                                                                                         #####
#####  7. Run the copy command for all four CSVs                                              #####
#####                                                                                         #####
#####      a. copy ks03.citizenbycid(cid,fname,lname,dob,status)                              #####
#####              from 'citizenbycid.csv' with delimiter=',';                                #####
#####                                                                                         #####
#####      b. copy ks03.citizenbyphone(phone,primarycid)                                      #####
#####              from 'citizenbyphone.csv' with delimiter=',';                              #####
#####                                                                                         #####
#####      c. copy ks03.misusealerts(misusedate,cid,misusetime,address,city,state,zip)        #####
#####              from 'misusealerts.csv' with delimiter=',';                                #####
#####                                                                                         #####
#####      d. copy ks03.pvcardusesbycid(cid,usenum,usedate,usetime,city,state,zip)            #####
#####              from 'pvcarduses.csv' with delimiter=',';                                  #####
#####                                                                                         #####
###################################################################################################

import getpass
from cassandra.query import SimpleStatement, BatchStatement
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import data_generator


# create authprovider object to connect to cassandra
#       - ask for username and password
#       - pass username and password to the PlainTextAuthProvider function as arguments
# the Cassandra cluster's contact point is 137.45.192.237
# create a cluster object
#       - pass authprovider object as an argument
# create a session object to connect to cluster to perform queries
#       - the session object should include the keyspace

def get_session():

    cass_username = input("Cassandra Username: ")
    cass_password = getpass.getpass("Cassandra Password: ")

    authprovider = PlainTextAuthProvider(username=cass_username, password=cass_password)
    cluster = Cluster(contact_points=['137.45.192.237'], port=9042, auth_provider=authprovider)

    # OR use the session.set_keyspace('ks03')
    # OR use session.execute('USE ks03')
    session = cluster.connect('ks03')
    return session

# ask_num_of_rows() -> int
#   - return the number of rows the user wants to insert
def ask_num_of_rows():
    return int(input('How many rows would you like to insert into each table?     '))

# get_SimpleStatement( table ) -> str
#   - return the SimpleStatement to insert into the correct table
def get_SimpleStatement(table):
    return "INSERT INTO citizenbycid (cid, fname, lname, dob, status) VALUES (%s,%s,%s,%s,%s)" if table=='citizenbycid' \
        else "INSERT INTO citizenbyphone (phone, primarycid) VALUES (%s,%s)" if table=='citizenbyphone' \
        else "INSERT INTO misusealerts (misusedate, cid, misusetime, address, city, state, zip) VALUES (%s,%s,%s,%s,%s,%s,%s)" if table=='misusealerts' \
        else "INSERT INTO pvcardusesbycid (cid, usenum, usedate, usetime, city, state, zip) VALUES (%s,%s,%s,%s,%s,%s,%s)"


# global constants used for every function after
SESSION = get_session()
NUM_OF_ROWS = ask_num_of_rows()


# insert_citizenbycid( ) -> List of generated CIDs
#   - insert data into citizenbycid table; generates UUID for citizen
#   - export the dataframe into a csv
def insert_citizenbycid():

    citizenbycid = data_generator.df_by_cid(NUM_OF_ROWS)
    
    citizenbycid.to_csv('citizenbycid.csv',header=['cid','fname','lname','dob','status'], index=False)
    
    batch = BatchStatement()
    # for every row of fake data, create insert statement for each row, then add them to the batch
    for row in citizenbycid.itertuples(index=False):
    
        batch.add(SimpleStatement(get_SimpleStatement('citizenbycid'))
            ,(row[0], row[1], row[2], row[3], row[4]))
            
    SESSION.execute(batch)
    
    print("Successfully inserted ", NUM_OF_ROWS, " records into CitizenByCID table............")
    
    return citizenbycid['CID'].to_list()


# insert_citizenbyphone( cids ) -> None
#   @ Pre-condition: Must insert into CITIZENBYCID table first to get the generated CIDs
#   - export the dataframe into a csv and insert data into the citizenbyphone table
def insert_citizenbyphone(cids):

    citizenbyphone = data_generator.df_by_phone(NUM_OF_ROWS,cids)
    
    citizenbyphone.to_csv('citizenbyphone.csv', header=['phone','primarycid'],index=False)
    
    batch = BatchStatement()
    
    for row in citizenbyphone.itertuples(index=False):
    
        batch.add(SimpleStatement(get_SimpleStatement('citizenbyphone'))
            ,(row[0], row[1]))
            
    SESSION.execute(batch)
    
    print("Successfully inserted ", NUM_OF_ROWS, " records into CitizenByPhone table..........")


# insert_misusealerts( cids ) -> None
#   @ Pre-condition: Must insert into CITIZENBYCID table first to get the generated CIDs
#   - export the dataframe into a csv and insert data into the MisuseAlerts Table
def insert_misusealerts(cids):

    misusealerts = data_generator.df_misusealerts(NUM_OF_ROWS,cids)
    
    misusealerts.to_csv('misusealerts.csv',header=['misusedate','cid','misusetime','address','city','state','zip'], index=False)
    
    batch = BatchStatement()
    
    for row in misusealerts.itertuples(index=False):
    
        batch.add(SimpleStatement(get_SimpleStatement('misusealerts'))
            ,(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
            
    SESSION.execute(batch)
    
    print("Successfully inserted ", NUM_OF_ROWS, " records into MisuseAlerts table............")


# insert_pvcarduses( cids ) -> None
#   @ Pre-condition: Must insert into CITIZENBYCID table first to get the generated CIDs
#   - export the dataframe into a csv and insert data into the PVcardusesbycid table
def insert_pvcarduses(cids):

    pvcarduses = data_generator.df_pvcarduses(NUM_OF_ROWS,cids)
    
    pvcarduses.to_csv('pvcarduses.csv',header=['cid','usenum','usedate','usetime','city','state','zip'], index=False)
    
    batch = BatchStatement()
    
    for row in pvcarduses.itertuples(index=False):
    
        batch.add(SimpleStatement(get_SimpleStatement('pvcardusesbycid'))
            ,(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
            
    SESSION.execute(batch)
    
    print('Successfully inserted ', NUM_OF_ROWS, ' records into PVcardUsesbyCID table.........')


# fulfill_query( ) -> None
#   - fulfill the users query request
def fulfill_query(query):
    rows = SESSION.execute(query)
    for row in rows:
        print(row)

# request_query( ) -> str
#   - ask the user to enter a query
def request_query():

    return input('Enter a query or type STOP:    ')
    
 
# multiple_queries( ) -> None or Recursive call
#   - asks the user to enter as many queries as desired
#   base case: if user types 'STOP'
#   recursive case: if user types another query

def multiple_queries():
    query = request_query()
    if query.upper() == 'STOP':
        return None
    else:
        fulfill_query(query)
        return multiple_queries()
        

def main():

    cids = insert_citizenbycid()
    insert_citizenbyphone(cids)
    insert_misusealerts(cids)
    insert_pvcarduses(cids)
    multiple_queries()
    
main()
