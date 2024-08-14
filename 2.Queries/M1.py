# coding=utf-8
import datetime
import time
import json
from pymongo import MongoClient
from faker import Faker

# Get today's date
today = datetime.datetime.now().date()

class Model1:
    """ Create Model1 with a function for data generation, inserting data, query 1, query 2, query 3, query 4. 
    Model1 has two types of documents, one for each class and referenced fields."""

    def data_generator(self, company_count=10, employee_count=50000):  
        """ Generates fake data for a given number of companies and employees.

        Args:
            company_count (int): Number of companies to generate data for.
            employee_count (int): Total number of employees across all companies.
        """

        fake = Faker()
        companies = []
        persons = []
        for _ in range(company_count):
            # Generating fake company information
            company_name = fake.company()
            company_email = "it-services@" + company_name.replace(" ", "").lower() + ".com"
            url = "www." + company_name.replace(" ", "").lower() + ".com"
            domain = company_name.replace(" ", "").lower() + ".com"

            company = {
                "name": company_name,
                "vat_number": fake.random_number(digits=9),
                "email": company_email,
                "url": url,
                "domain": domain
            }
            
            # Generating fake employees for each company
            for _ in range(int(employee_count/company_count)):
                birthday = fake.date_time()

                # Calculate age
                age = today.year - birthday.year - ((today.month, today.day) < (birthday.month, birthday.day))

                # Generating fake employee information
                full_name = fake.name()
                first_name = full_name.split()[0]  
                employee_email = full_name.replace(" ", "").lower() + "@" + company_name.replace(" ", "").lower() + ".com"

                person = {
                    "first_name": first_name,
                    "full_name": full_name,
                    "sex": fake.random_element(["M", "F"]),
                    "birthdate": birthday.isoformat(),
                    "age": age,
                    "company_name": company["name"],
                    "company_email": employee_email
                }
                persons.append(person)
            companies.append(company)
        return companies, persons
    
    def insert_data(self, companies, persons):
        client = MongoClient('localhost',27017)
        db = client['proj1M1']  
        collection_companies = db['companies']
        collection_persons = db['persons']
        
        # Drop the persons collection if it exists
        if 'persons' in db.list_collection_names():
            db.drop_collection('persons')

        # Drop the companies collection if it exists
        if 'companies' in db.list_collection_names():
            db.drop_collection('companies')
        
        collection_companies.insert_many(companies)
        collection_persons.insert_many(persons)
        
        client.close()

    def query_q1(self):
        """ For each person, retrieve their full name and their company's name.
        """
        # Establish connection to MongoDB
        client = MongoClient('localhost', 27017)
        db = client['proj1M1'] 
        collection_persons = db['persons']
        
        # Start timing the query
        start_time = time.time()
                
        # Query to retrieve full name and company name for each person
        result = collection_persons.find({}, {"_id": 0, "full_name": 1, "company_name": 1})        

        # Calculate query execution time
        query_time = time.time() - start_time
        
        # Print results
        for doc in result:
            print(doc)
        
        # Print query execution time
        print("--- %s seconds ---" % query_time)
        
        # Close MongoDB connection
        client.close()

    def query_q2(self):
        """ For each company, retrieve its name and the number of employees.
        """
        # Establish connection to MongoDB
        client = MongoClient('localhost', 27017)
        db = client['proj1M1'] 
        collection_persons = db['persons']
        
        # Start timing the query
        start_time = time.time()
        
        # Perform the aggregation query to count employees per company
        result = collection_persons.aggregate([
            {"$group": {
                "_id": "$company_name",
                "count": {"$sum": 1}
            }}
        ])
        
        # Calculate query execution time
        query_time = time.time() - start_time
        
        # Print results
        for doc in result:
            print(f"{doc['_id']}: {doc['count']} employees")
        
        # Print query execution time
        print("--- %s seconds ---" % query_time)
        
        # Close MongoDB connection
        client.close()

    def query_q3(self):
        """ For each person born before 1988, update their age to “30”.
        """
        # Establish connection to MongoDB
        client = MongoClient('localhost', 27017)
        db = client['proj1M1']  
        collection_persons = db['persons']
        
        # Start timing the query
        start_time = time.time()
        
        # Update age for persons born before 1988
        collection_persons.update_many(
            {"$expr": {"$lt": [{"$toInt": {"$substr": ["$birthdate", 0, 4]}}, 1988]}},
            {"$set": {"age": 30}}
        )
        
        # Calculate query execution time
        query_time = time.time() - start_time
        
        # Retrieve and print information for the first 10 people
        first_people = collection_persons.find().limit(10)
        for person in first_people:
            print(person["full_name"] + " (Age:", str(person["age"]) + ")" + ", Birthdate:" + str(person["birthdate"]))
        
        # Print query execution time
        print("--- %s seconds ---" % query_time)
        
        # Close MongoDB connection
        client.close()

    def query_q4(self):
        """ For each company, update its name to include the word “Company”.
        """
        # Establish connection to MongoDB
        client = MongoClient('localhost', 27017)
        db = client['proj1M1'] 
        collection_companies = db['companies']
        
        # Start timing the query
        start_time = time.time()
        
        # Update company names to include the word "Company"
        collection_companies.update_many({}, [{"$set": {"name": {"$concat": ["$name", " Company"]}}}])
        
        # Calculate query execution time
        query_time = time.time() - start_time        

        # Retrieve and print updated company names
        documents = collection_companies.find().limit(10)
        print("Company names after update:")
        for doc in documents:
            print("Company:",doc['name'])

        # Print query execution time
        print("--- %s seconds ---" % query_time)
        
        # Close MongoDB connection
        client.close()