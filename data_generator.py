from faker import Faker
import string
import numpy as np
import pandas as pd
from random import choice, randint, random
from datetime import datetime, timedelta
from itertools import chain
import datetime
import uuid
import csv
 
fake = Faker()
status_list = ['CLEARED','FAILED']
 
def get_choice(lst_choice,num_choices):
  # Pass a list of items and choose n random items from that list
  lst = [choice(lst_choice) for i in range(num_choices)]
  return lst
  
def get_uuids(len_lst):
    return [uuid.uuid4() for i in range(len_lst)]
    
def get_timeuuids(len_lst):
    return [uuid.uuid1() for i in range(len_lst)]
 
def gen_DOBs(len_lst):
  DOB = [fake.date_of_birth(minimum_age=12,maximum_age=100).isoformat() for i in range(len_lst)]
  return DOB

def gen_fakeFNames(len_lst):
# generate random first name for n
    lst = [fake.first_name() for i in range(len_lst)]
    return lst
 
def gen_fakeLNames(len_lst):
  #generate random last name for n
  lst = [fake.last_name() for i in range (len_lst)]
  return lst
 
def gen_fakePhones(len_lst):
  lst = [fake.phone_number() for i in range(len_lst)]
  return lst
 
def gen_date(len_lst):
    lst = [fake.date() for i in range(len_lst)]
    return lst
 
def gen_time(len_lst):
    lst = [fake.time() for i in range(len_lst)]
    return lst
 
def gen_address(len_lst):
    lst = [fake.street_address() for i in range(len_lst)]
    return lst
 
def gen_state(len_lst):
    lst = [fake.state_abbr() for i in range(len_lst)]
    return lst
 
def gen_city(len_lst):
    lst = [fake.city() for i in range(len_lst)]
    return lst
 
def gen_zip(len_lst):
    lst = [fake.postcode() for i in range(len_lst)]
    return lst

#####################################
###        CitizenByCID           ###
#####################################
#  CID                  #     PK    #
#  Fname                #           #
#  Lname                #           #
#  DOB                  #           #
#  Status               #           #
#####################################

# df_by_cid ():
#   - return the data in a dataframe for Citizen_By_CID table

# this is the only table where a UUID shoud be generated,
#   all other tables reference this UUID
def df_by_cid(num_of_rows):
    cids = get_uuids(num_of_rows)
    fnames = gen_fakeFNames(num_of_rows)
    lnames = gen_fakeLNames(num_of_rows)
    birthdates = gen_DOBs(num_of_rows)
    statuses = get_choice(status_list,num_of_rows)
    return pd.DataFrame(data={'CID' : cids 
                            , 'Fname' : fnames
                            , 'Lname':  lnames
                            , 'DOB' : birthdates
                            , 'Status' : statuses})

#####################################
###        CitizenByPhone         ###
#####################################
#   Phone               #     PK    #
#   PrimaryCID          #           #
#   CIDlist             #           #
#####################################

# use the CIDS from the citizenbycid dataframe to 
#   fill in the primary cid column for this table

# when calling the dataframes, make sure to add CID
#       to this table once the citizenbyid df is available

def df_by_phone(num_of_rows, cids):
    phones = gen_fakePhones(num_of_rows)
    return pd.DataFrame(data={'Phone' : phones
                            ,'primarycid' : cids})
    

#####################################
###         MisuseAlerts          ###
#####################################
#   MisuseDate          #   PK, K   #
#   CID                 #   PK, C   #
#   Misuse Time         #   PK, C   #
#   Address             #           #
#   City                #           #
#   State               #           #
#   Zipcode             #           #
#####################################

# # use the CIDS from the citizenbycid dataframe to 
#   fill in the primary cid column for this table

# when calling the dataframes, make sure to add CID
#       to this table once the citizenbyid df is available

def df_misusealerts(num_of_rows, cids):
    dates = gen_date(num_of_rows)
    times = gen_time(num_of_rows)
    addresses = gen_address(num_of_rows)
    cities = gen_city(num_of_rows)
    states = gen_state(num_of_rows)
    zipcodes = gen_zip(num_of_rows)
    return pd.DataFrame(data={'MisuseDate' : dates
                            , 'CID' : cids
                            , 'MisuseTime' : times
                            , 'Address' : addresses
                            , 'City' : cities
                            , 'State' : states
                            , 'Zip' : zipcodes})
                            
#####################################
###        PVCardUsesByCID        ###
#####################################
#   CID                 #  PK, K    #
#   UseNum              #  PK, C    #
#   UseDate             #           #
#   UseTime             #           #
#   City                #           #  
#   State               #           #
#   Zipcode             #           #
#####################################

def df_pvcarduses(num_of_rows, cids):
    use_ids = get_timeuuids(num_of_rows)
    dates = gen_date(num_of_rows)
    times = gen_time(num_of_rows)
    cities = gen_city(num_of_rows)
    states = gen_state(num_of_rows)
    zipcodes = gen_zip(num_of_rows)
    return pd.DataFrame(data={'CID' : cids
                            , 'UseNum' : use_ids
                            , 'UseDate' : dates
                            , 'UseTime' : times
                            , 'City' : cities
                            , 'State' : states
                            , 'Zip' : zipcodes})

    
                            

    
