#Script to automatically download a google sheets file and write into new table on AWS
#Created by Robb Streicher 3/15/2017
#Note you need to register and test the Google Sheets API before this script works
#Follow the tutorial here to set up https://developers.google.com/sheets/api/quickstart/python

from __future__ import print_function
import httplib2
import os
import decimal
import datetime
import pymssql
import _mssql
import uuid
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
conn = pymssql.connect(server='YOURSERVER', user='YOUR USERNAME', password='YOURPW', database='YOURDB')

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

#this is the name of your client_secret json file you get from google
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'

#this just cleans up the quotes and allows you to insert columns into sql that have a single quote in the data
#i know this sucks
def cleaner(stuff):
     writer =str(stuff).replace("[","").replace("]","").replace('"',"'")
     writer =writer.replace("'","''")
     writer =writer.replace("'', ''","', '")
     writer=writer[:-1]
     writer=writer[1:]
     return writer 
def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def main():
    #This calls the google sheets API and downloads all the data
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)
    #put your spreadsheetID in the line below
    spreadsheetId = 'YOURSPREADSHEETID'


    #you tell it which sheet to pull here. By default it uses the firstsheet sheets[0]
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheetId).execute()
    sheets = sheet_metadata.get('sheets', '')
    title = sheets[0].get("properties", {}).get("title")
    
    #set the columns to read in this case it's the first 25
    rangeName=str(title)+"!A1:Y"
    print(title)
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=rangeName).execute()
    #Create your table in SQL Here. It reads the columns of the sheet and assigns them all a type of varchar(255)
    values = result.get('values', [])
    insrtvals =[]
    of = open ("dlded_sheet.csv","w")
    hr=0
    now = str(datetime.datetime.now())
    now = now.replace("-","_").replace(" ","_").replace(":","_")
    now = now[:-7]
    hlen =0
    tabn= str(title).replace("'","")+"_"+now
    createstate ="create table "+tabn+" ("
    writer =""
    for row in values:
            filler =len(row)
            rowar =[]
            rowar.append(row)
            if hr==0:
                while hr <filler:
                    createstate = createstate +(" ["+str(row[hr])+"] varchar(255),")
                    hr=hr+1
                    hlen = len(row)
                createstate = createstate[:-1]
                createstate =createstate +");"
               # print (createstate)
            while (filler <hlen):
                rowar.append('''''')
                filler = filler+1


            of.write(cleaner(rowar))
            of.write("\n")
    of.close()
    
    cursor = conn.cursor()
    try:
        cursor.execute(createstate)
        conn.commit()
        print("New Table "+tabn+" Created Succesfully")
        print ("Now inserting rows...")
    
    except:
        print("Error Creating table")           


    #start inserting your rows here
    if not values:
        print('No data found.')
    rs=0
    for row in values:
        if rs==0:
            print("skipping header row")
        else:
            filler =len(row)
            rowar =[]
            rowar.append(row)
            if filler==hlen:
                ins =cleaner(rowar)
            while (filler <hlen):
                rowar.append('''''')
                filler = filler+1
                ins =cleaner(rowar)
            try:
                cursor = conn.cursor()
                cursor.execute("insert into "+tabn+" values("+ins+");")
                conn.commit()
                #print("row inserted")
            except:
                print("error inserting this row\n"+ins) 
        rs=rs+1  

    conn.close()

#Finshed
print("table updated to match google sheets")
            

           
if __name__ == '__main__':
    main()
