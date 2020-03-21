# -*- coding: utf-8 -*-
"""
Created on Sat Feb 29 10:24:18 2020

@author: Johan
"""

# launch the first local mongo server
# In a cmd prompt, input your mongodb directory and launch it:
#   cd \Users\Johan\Downloads\mongodb-win32-x86_64-2012plus-4.2.2\bin
#   mongod.exe --dbpath C:\Data\mongoDB


# already used:
import requests

# new packages:
#pip install pymongo
import pymongo



##Test
#db.my_collection
#
#db.my_collection.insert_one({"x": 10}).inserted_id
#db.my_collection.insert_one({"x": 8}).inserted_id
#db.my_collection.insert_one({"x": 11}).inserted_id
#
#db.my_collection.find_one()
#
#for item in db.my_collection.find():
#    print(item["x"])
#
#db.list_collection_names()



def Insert_in_Transaction(db, params):
    """
    Insert in the database Transaction the documents with the values given in parameters
    Print an error if the insertion does not succeed
    """
    
    try:        
        db.Transaction.insert_one(
                {
                        "Open time": params[0], 
                        "High": params[1], 
                        "Low": params[2], 
                        "Open": params[3], 
                        "Close": params[4], 
                        "Volume": params[5], 
                        "Quote asset volume": params[6], 
                        "Weighted average": params[7]
                }
        )
        
    except Exception as e:
        print(e)



def Print_Items(db):
    """
    Print all the documents in the database
    """
    
    for item in db.Transaction.find():
        print(item)



# Queries:
    
def Count_Documents(db):
    """
    Return the total number of documents
    """
    
    count = db.Transaction.estimated_document_count()
    print("Number of documents in the database Transaction: " + str(count) + ".\n")
    return count



def Close_Average(db):
    """
    Return the average of the Close prices
    """
    
    average = 0
    n = 0
    
    for document in db.Transaction.find():
        average = average + float(document.get("Close"))
        n = n + 1
        
    return average / n



def Find_Weighted_Average(db):
    """
    Print the documents chosen with their argument 'Weighted average'
    """
    
    #db.Transaction.find_one({"Weighted average": {"$gt": 0}})
    
    wa = float(input("Write the weighted average you want to find (e.g. " + str(db.Transaction.find_one().get("Weighted average")) + "): \n"))
    
    choice = int(input(" 1 - Print one transaction with the given weighted average\n 2 - Print all the documents with the given weighted average\n"))
    
    if choice == 1:
        ## Find a Transaction with a given weighted average
        db.Transaction.find_one({"Weighted average": wa})
    
    if choice == 2:
        ## Find all the documents with the given weighted average
        for document in db.Transaction.find({"Weighted average": wa}):
            print(document)
            


def gt_lt_Close(db):
    """
    Print the documents with a Close price between the limits given by the user
    """
    
    gt = input("Input the minimum close price (by default 0.02147): ")
    lt = input("Input the maximum close price (by default 0.02148): ")
    
    if gt == "":
        gt = "0.02147"
        
    if lt == "":
        lt = "0.02148"
    
    for document in db.Transaction.find( { "$and": [ { "Close": {"$gt": gt} }, { "Close": {"$lt": lt} } ] } ):
        print(document)



def Mongodb_Connection():
    """
    Create the connection and drop the database if needed
    """
    
    client = pymongo.MongoClient("localhost", 27017)
    db = client.test


    if db.Transaction.estimated_document_count() != 0:
        """
        To make a new test, the database is cleared if not empty
        """
    
        db.command("dropDatabase")
        
    return db



def Request(db):
    """
    Ask values to the API and launch the `Insert_in_Transaction()` function
    """
    
    pair = input("Input the wanted pair (by default: ETHBTC): ")
    
    if pair == "":
        pair = "ETHBTC"
        print("Your pair is: " + pair)
    
    duration = input("Input the duration, i.e. 1m, 3m, 5m, 15m, 30m, 1h... (by default 30m): ")
        
    if duration == "":
        duration = "30m"
        print("Your duration is: " + duration)

    
    r = requests.get("https://api.binance.com/api/v3/klines?symbol=" + pair + "&interval=" + duration + "&limit=500")
    
    
    # Insert values in mongodb
    for i in range(0, 500):
        params = [''] * 8
        params[0] = r.json()[i][0]                                # Open time (date)
        params[1] = r.json()[i][2]                                # High
        params[2] = r.json()[i][3]                                # Low
        params[3] = r.json()[i][1]                                # Open
        params[4] = r.json()[i][4]                                # Close
        params[5] = r.json()[i][5]                                # Volume
        params[6] = r.json()[i][7]                                # Quote asset volume
        params[7] = (float(params[1]) + float(params[2])) / 2     # Weighted average = (High+Low)/2
        
        Insert_in_Transaction(db, params)
        
        if i == 500 - 1:
            print("\nData inserted successfully! \n")



def Menu_Mongodb():
    """
    Menu where the user can access the functions (Console)
    """
    
    print("\n /!\ Introduction /!\ \nSome values are sometimes asked to the user. You can skip it when 'default' values are already input simply by pressing 'Enter'. ")
    
    db = Mongodb_Connection()
    
    exit=1
    
    print("\nSelect the number of the desired option in the Menu: ")
    
    while exit != 0:
        
        exit = int(input(""" --- Menu ---
 1 - Insert the 500 most recent documents in the database
 2 - Print all the items in the database
 3 - Count the total number of documents
 4 - Find one or several document with its/their argument 'Weighted average'
 5 - Find the documents with a Close price between a minimum and a maximum value
 0 - Exit
"""))
        
        if exit == 1:
            Request(db)
            
        elif exit == 2:
            Print_Items(db)
            
        elif exit == 3:
            Count_Documents(db)
            
        elif exit == 4:
            Find_Weighted_Average(db)
            
        elif exit == 5:
            gt_lt_Close(db)

        else:
            print("\n--- End of the program! ---")



Menu_Mongodb()














