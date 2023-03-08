from notion_client import Client
from psycopg2 import Error
import psycopg2

class Notion_Collector: 
    def __init__(self, host, database, user, password, port, database_id='DB_ID', auth_="SECRET"):
        self.connection = psycopg2.connect('host= {0} dbname= {1} user= {2} password= {3} port = {4}'.format(host, database, user, password, port))
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

        #NOTION DATABASE INSTANCE 
        self.notionDB = Notion_DataBase(database_id,auth_)

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

    def insert_into_db(self, values):
        try:
            # CLEAR TABLE
            self.cursor.execute('TRUNCATE notion_db;')
            # INSERT
            self.cursor.execute(values)

            print(f"  - Updated notion_db table!")

        except (Exception, Error) as error:
            print("Error while connecting to DB", error)

    def update_notion_data(self):
        
        # Extract the header names from the database schema
        headers = self.notionDB.get_headers()

        insert = 'INSERT INTO notion_db ('
        

        # Print the headers
        for header in headers:
            if header != headers[-1]:
                insert += '"' + header.replace('\ufeff','') + '",'
            else: 
                insert += '"' + header.replace('\ufeff','') + '") VALUES '
                
        # Retrieve all the data from the database
        results = self.notionDB.get_results()

        insert_data = ""

        # Print each row in the console
        for result in results:
            row_data = []
            
            for prop in result["properties"]:
                prop_data = result["properties"][prop]
                if prop_data.get("title"):
                    row_data.append(prop_data["title"][0]["text"]["content"])
                elif prop_data.get("rich_text"):
                    row_data.append(prop_data["rich_text"][0]["text"]["content"])
                elif prop_data.get("select"):
                    row_data.append(prop_data["select"]["name"])
                elif prop_data.get("multi_select"):
                    values = [v["name"] for v in prop_data["multi_select"]]
                    row_data.append(", ".join(values))
                elif prop_data.get("date"):
                    row_data.append(prop_data["date"]["start"])
                elif prop_data.get("formula"):
                    row_data.append(prop_data["formula"]["string"])
                else:
                    row_data.append('')
            
            if result != results[-1]:
                insert_data += str(row_data).replace('[','(').replace(']','),')
            else: 
                insert_data += str(row_data).replace('[','(').replace(']',');')

        self.insert_into_db(insert + insert_data)

class Notion_DataBase: 
    def __init__(self, database_id, auth_):
        self.notion = Client(auth=auth_)
        self.database_id = database_id

    def get_headers(self):

        database = self.notion.databases.retrieve(self.database_id)

        return [prop["name"] for prop in database["properties"].values()]
        
    def get_results(self):
        return self.notion.databases.query(self.database_id).get("results")
