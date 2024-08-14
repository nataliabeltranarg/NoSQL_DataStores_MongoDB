# coding=utf-8
import datetime
import time
import json
from pymongo import MongoClient
from faker import Faker

# Get today's date
today = datetime.datetime.now().date()

class Model3:
    """ Create Model3 with a function for data generation, inserting data, query 1, query 2, query 3, query 4. 
    Model3 has one document for “Company” with “Person” as embedded documents."""

    def data_generator(self, company_count=10, employee_count=50000):
        """ Generates fake data for a given number of companies and employees.

        Args:
            company_count (int): Number of companies to generate data for.
            employee_count (int): Total number of employees across all companies.
        """

        fake = Faker()
        companies = []

        # Generate fake data for each company
        for _ in range(company_count):

            # Company Information
            company_name = fake.company()
            vat_number = fake.random_number(digits=9)
            email = "it-services@" + company_name.replace(" ", "").lower() + ".com"
            url = "www." + company_name.replace(" ", "").lower() + ".com"
            domain = company_name.replace(" ", "").lower() + ".com"

            # Company dictionary to store company information
            company = {
            "name": company_name,
            "vatNumber": vat_number,
            "email": email,
            "url": url,
            "domain": domain,
            }

            # Generate fake data for employees of the company
            for _ in range(int(employee_count/company_count)):
                
                # Calculate age based on randomly generated birthdate
                birthday = fake.date_time()
                age = today.year - birthday.year - ((today.month, today.day) < (birthday.month, birthday.day))

                # Extract first name from full name 
                full_name = fake.name()
                first_name = full_name.split()[0] 

                # Generate company email based on employee's full name and company domain
                company_email = full_name.lower().replace(" ", "") + "@" + domain

                # Employee dictionary to store employee information
                employee = {
                    "firstName": first_name,
                    "full_name": full_name,
                     "sex": fake.random_element(["M", "F"]),
                    "birthdate": birthday.isoformat(),
                    "age": age,
                    "ssn": fake.ssn(),
                    "address": fake.address(),
                    "companyEmail": company_email
                }
                
                # Append employee to the company's list of employees
                company.setdefault("employees", []).append(employee)
            
            # Append company to the list of companies
            companies.append(company)
        return companies

    def insert_data(self, companies):
        client = MongoClient('127.0.0.1:27017')
        db = client['proj1M3']  
        collection_companies = db['companies']

        # Drop the collection if it exists
        if 'companies' in db.list_collection_names():
            db.drop_collection('companies')

        collection_companies.insert_many(companies)
        client.close()

    def query_q1(self):
        """ For each person, retrieve their full name and their company's name.
        """
        # Establish connection to MongoDB
        client = MongoClient('localhost', 27017)
        db = client['proj1M3']  
        collection_companies = db['companies']
        
        # Start timing the query
        start_time = time.time()
        
        # Query to retrieve full name and company name for each employee
        result = collection_companies.find({}, {"_id": 0, "name": 1, "employees.full_name": 1})
        
        # Calculate query execution time
        query_time = time.time() - start_time
        
        # Print results
        for company in result:
            company_name = company["name"]
            employees = company["employees"]
            for employee in employees:
                employee_name = employee["full_name"]
                print("Full Name: {}, Company: {}".format(employee_name, company_name))
        
        # Print query execution time
        print("--- %s seconds ---" % query_time)
        
        # Close MongoDB connection
        client.close()

    def query_q2(self):
        """ For each company, retrieve its name and the number of employees.
        """
        # Establish connection to MongoDB
        client = MongoClient('localhost', 27017)
        db = client['proj1M3']
        collection_companies = db['companies']
        
        # Start timing the query
        start_time = time.time()

        # Aggregate to count the number of employees per company
        pipeline = [
            {"$unwind": "$employees"},
            {"$group": {"_id": "$name", "count": {"$sum": 1}}}
        ]
        result = collection_companies.aggregate(pipeline)

        # Calculate query execution time
        query_time = time.time() - start_time

        # Print results
        for company in result:
            company_name = company["_id"]
            num_employees = company["count"]
            print(f"{company_name}: {num_employees} employees")

        # Print query execution time
        print("--- %s seconds ---" % query_time)
        client.close()

    def query_q3(self):
        """ For each person born before 1988, update their age to “30”.
        """
        # Establish connection to MongoDB
        client = MongoClient('localhost', 27017)
        db = client['proj1M3']  
        collection_companies = db['companies']

        start_time = time.time()

        # Retrieve all documents from the collection
        companies = collection_companies.find({})

        # Iterate through each company document
        for company in companies:
            # Iterate through each employee of the company
            for employee in company['employees']:
                # Extract the birthdate year and convert it to an integer
                birth_year = int(employee['birthdate'][:4])

                # Check if the birthdate year is before 1988
                if birth_year < 1988:
                    # Update the age to 30
                    employee['age'] = 30

            # Update the document in the collection
            collection_companies.replace_one({'_id': company['_id']}, company)

        # Calculate query execution time
        query_time = time.time() - start_time
        
        # Retrieve and print information for the first 10 employees of the first company
        companies = collection_companies.find({}).limit(1)
        for company in companies:
            count = 0  # Counter to keep track of printed employees
            for person in company["employees"]:
                if count < 10:  # Print maximum 10 employees
                    print(person["full_name"] + " (Age:", str(person["age"]) + ")" + ", Birthdate:" + str(person["birthdate"]))
                    count += 1
                else:
                    break  # Break the loop after printing 10 employees

        # Print query execution time
        print("--- %s seconds ---" % query_time)

        # Close MongoDB connection
        client.close()

    def query_q4(self):
        """ For each company, update its name to include the word “Company”.
        """
        # Establish connection to MongoDB
        client = MongoClient('localhost', 27017)
        db = client['proj1M3']  
        collection_companies = db['companies']
        start_time = time.time()
        
        # Query to update company names
        cursor = collection_companies.find({})
        for company in cursor:
            # Update each company name
            new_name = company['name'] + " Company"
            collection_companies.update_one({"_id": company["_id"]}, {"$set": {"name": new_name}})
        
        # Calculate query execution time
        query_time = time.time() - start_time

        # Print updated company names
        print("Company names after update:")
        cursor_after = collection_companies.find({}, {"name": 1})
        for company in cursor_after:
            print("Company:",company['name'])

        # Print query execution time
        print("--- %s seconds ---" % query_time)
        
        # Close MongoDB connection
        client.close()
