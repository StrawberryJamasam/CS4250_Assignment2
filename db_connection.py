#-------------------------------------------------------------------------
# AUTHOR: Jane Barnett
# FILENAME: db_connection.py
# SPECIFICATION: This code connects to a database and has methods to perform sql queries
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import MongoClient
import datetime
import string
from collections import Counter

def connectDataBase():

    # Create a database connection object using psycopg2
    
    DB_NAME = "Assignment2_DB"
    DB_HOST = "localhost"
    DB_PORT = 27017
    
    try:
        
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db

    except:
        print("Database not connected successfully")


def createCategory(cur, catId, catName):

    # Insert a category in the database
    Category = {"id_cat": catId,
                "name": catName
                }
    
    cur.insert_one(Category)


def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    cat = cur.find_one({"name":docCat})
    id_cat = cat['id_cat']

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    num_chars = len(docText) - docText.count(' ') - docText.count('\"') - docText.count('\'') - docText.count('.') \
        - docText.count('!') - docText.count('?') - docText.count(',') - docText.count(';') - docText.count(':')

    Document = {"id_doc": docId,
                "text": docText,
                "title": docTitle,
                "num_chars": num_chars,
                "date": docDate,
                "id_cat": id_cat
                }
    
    cur.insert_one(Document)
    
    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    doc_mod = Document.translate(str.maketrans('', '', string.punctuation))
    terms_divided = list(doc_mod.split(" "))
    terms = [x.lower() for x in terms_divided]
    terms_unique = list(set(terms))
    
    # checking if index db exists
    client = MongoClient()
    dbnames = client.list_database_names()
    if 'Index' not in dbnames:
        for x in terms_unique:
            Index = {"term": x,
                     "id_doc": docId,
                     "term_count": 0}
            
            cur.insert_one(Index)
    
    else:
        for x in terms_unique:
            term = cur.find_one({"term": x, "id_doc": docId})
            if term:
                ex = True
            else: 
                Index = {"term": x,
                        "id_doc": docId,
                        "term_count": 0}

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database

    term_counts = getTermCounts(terms)
    
    for x in terms_unique:
        count = term_counts[x]
        
        current_term = cur.find_one({"term": x, "id_doc": docId})
        current_term_count = current_term['term_count']
        current_term_count += count
        
        cur.update_one({"term_count": current_term_count}, current_term)


def getTermCounts(cur, text):
        terms_counts = Counter(text)
        return terms_counts


def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here

    # 2 Delete the document from the database
    print("Under Construction...")


def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)


def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    print("Under Construction...")