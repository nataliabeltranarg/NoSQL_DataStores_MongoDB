# coding=utf-8
import datetime
import time
import json
from pymongo import MongoClient
from faker import Faker

# Get today's date
today = datetime.datetime.now().date()

class Model2:
    """ Create Model2 with a function for data generation, inserting data, query 1, query 2, query 3, query 4. 
    Model2 has one document for “Person” with “Company” as embedded document."""

    def data_generator(self, company_count=10, employee_count=50000):
        """ Generates fake data for a given number of companies and employees.

        Args:
            company_count (int): Number of companies to generate data for.
            employee_count (int): Total number of employees across all companies.
        """

        fake = Faker()
        persons = []
        employees_per_company = int(employee_count / company_count)
        companies = []

        # Generate fake company names
        for _ in range(company_count):
            companies.append(fake.company())

        # Generate fake data for each company and its employees
        for company_name in companies:
            for _ in range(employees_per_company):
                # Generate fake birthday
                birthday = fake.date_time()

                # Calculate age
                age = today.year - birthday.year - ((today.month, today.day) < (birthday.month, birthday.day))

                # Generate company information
                company_vat_number = fake.random_number(digits=9)
                company_email = "it-services@" + company_name.replace(" ", "").lower() + ".com"
                company_url = "www." + company_name.replace(" ", "").lower() + ".com"
                company_domain = company_name.replace(" ", "").lower() + ".com"

                company = {
                    "name": company_name,
                    "email": company_email,
                    "url": company_url,
                    "domain": company_domain,
                    "vat_number": company_vat_number
                }

                # Generate employee information
                full_name = fake.name()
                first_name = full_name.split()[0]
                person = {
                    "first_name": first_name,
                    "full_name": full_name,
                    "sex": fake.random_element(["M", "F"]),
                    "birthdate": birthday.isoformat(),
                    "age": age,
                    "company_name": company_name,
                    "company_email": first_name.lower() + "@" + company_domain,
                    "company": company
                }

                persons.append(person)

        return persons
        
    def insert_data(self, persons):
        client = MongoClient('127.0.0.1:27017')
        db = client['proj1M2']  
        collection_persons = db['persons']

        # Drop the collection if it exists
        if 'persons' in db.list_collection_names():
            db.drop_collection('persons')

        collection_persons.insert_many(persons)
        client.close()

    def query_q1(self):
        """ For each person, retrieve their full name and their company's name.
        """
        # Establish connection to MongoDB
        client = MongoClient('localhost', 27017)
        db = client['proj1M2']  
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
        db = client['proj1M2']  
        collection_persons = db['persons']
        
        # Start timing the query
        start_time = time.time()
        
        # Aggregate to count the number of employees per company
        result = collection_persons.aggregate([
            {"$group": {"_id": "$company.name", "count": {"$sum": 1}}}
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
        db = client['proj1M2']  
        collection_persons = db['persons']
        start_time = time.time()
        
        # Query to update age for persons born before 1988
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
        db = client['proj1M2']  
        collection_persons = db['persons']
        start_time = time.time()
        
        # Query to update company names
        pipeline = [
            {"$set": {"company.name": {"$concat": ["$company.name", " Company"]}}},  
            {"$out": "persons"}
        ]
        collection_persons.aggregate(pipeline, session=None)
        
        # Calculate query execution time
        query_time = time.time() - start_time
        
        # Retrieve and print updated company names
        result = collection_persons.find({}, {'company.name': 1})
        unique_company_names = set()

        for doc in result:
            unique_company_names.add(doc['company']['name'])

        print("Unique Company Names:")
        for name in unique_company_names:
            print("Company:", name)

        # Print query execution time
        print("--- %s seconds ---" % query_time)

        # Close MongoDB connection
        client.close()
