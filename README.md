# Big Data Management: Data Modeling and Query Performance Analysis using MongoDB

## Project Overview 
This project explores various data modeling techniques in MongoDB and analyzes the performance of different query operations across these models. It involves creating and comparing three different MongoDB document models, performing CRUD operations, and measuring their execution times to evaluate efficiency. 

## Objectives: 
- **Data Modeling:** Implemented three different document models in MongoDB to represent a simple organizational structure of persons and companies. 
- **Data Generation:** Use the Faker library to generate realistic random data for the models
- **Query Implementation:** Executed specific queries across the models to retrieve and update data 
- **Performance Analysis:** Measured and compared the performance of queries across the three models to draw conclusions about data modeling best practices in MongoDB.

  ## Project Structure
  - ***'M1.py'***: Implementation of Model 1 which was two types of documents, one for each class and referenced fields.
  - ***'M2.py'***: Implementation of Model 2 which was one document for *'Person'* with *'Company'* as embedded documents. 
  - ***'M3.py'***: Implementation of Model 3 which was one document for *'Company'* with  *'Person'* as embedded documents
  - ***'BigDataManagement_MongoDB_notebook.ipynb'***: Contains the queries to check efficiency of the models. 

## How to navigate the repository
```bash 
├── 1.Data
│   ├── COMPANIES.json
│   ├── CompanyEmbed.json
│   ├── EMPLOYEES.json
│   └── EmpoyeesEmbed.json
├── 2.Models
│   ├── M1.py
│   ├── M2.py
│   ├── M3.py
│   └── __pycache__
├── 3.Document
│   ├── BigDataManagement_MongoDB_notebook.ipynb
│   └── BigDataManagement_MongoDB_report.pdf
└── README.md
```
