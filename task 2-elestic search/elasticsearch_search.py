# elasticsearch_script.py

from elasticsearch  import Elasticsearch, helpers
import pandas as pd
from elasticsearch import NotFoundError

# Initialize Elasticsearch client
es = Elasticsearch("http://localhost:8989", verify_certs=False)

# ========== Function Definitions ==========

# Function to create an index
def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection '{p_collection_name}' created.")
    else:
        print(f"Collection '{p_collection_name}' already exists.")

# Function to index data excluding a specified column
def indexData(p_collection_name, p_exclude_column, data_path='Employee.csv'):
    df = pd.read_csv(data_path , encoding='ISO-8859-1')
    #print("Columns in CSV:", df.columns.tolist())
    

    if p_exclude_column in df.columns:
        df = df.drop(columns=[p_exclude_column])

    actions = [
    {
        "_index": v_nameCollection,
        "_id": row['Employee ID'] if pd.notna(row['Employee ID']) else f"default_id_{index}",  # Fallback to unique default ID
        "_source": row.to_dict()
       
    }
    for index, row in df.iterrows() if pd.notna(row['Employee ID'])  # Only include rows with valid IDs
]
    #helpers.bulk(es, actions)
    try:
        success, failed = helpers.bulk(es, actions, raise_on_error=False, stats_only=True)
        print(f"Successfully indexed: {success}, Failed to index: {failed}")

    except helpers.BulkIndexError as e:
        for err in e.errors:
            print("Failed to index document:", err)  # Print each error message
    print(f"Data indexed into collection '{p_collection_name}', excluding '{p_exclude_column}'.")


# Function to search by a specific column value
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    res = es.search(index=p_collection_name, body=query)
    return res['hits']['hits']

# Function to get employee count
def getEmpCount(p_collection_name):
    count = es.count(index=p_collection_name)
    return count['count']

# Function to delete an employee by ID
def delEmpById(p_collection_name, p_employee_id):
    # Check if the document exists
    if es.exists(index=p_collection_name, id=p_employee_id):
        try:
            # Try to delete the document
            es.delete(index=p_collection_name, id=p_employee_id)
            print(f"Document with ID {p_employee_id} successfully deleted.")
        except NotFoundError:
            print(f"Document with ID {p_employee_id} was not found for deletion.")
    else:
        print(f"Document with ID {p_employee_id} was not found for deletion.")
        


# Function to get employee count by department (facet search)
def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "department_count": {
                "terms": {"field": "Department.keyword"}
            }
        }
    }
    res = es.search(index=p_collection_name, body=query)
    return res['aggregations']['department_count']['buckets']

# ========== Function Execution ==========

# Define collection names
v_nameCollection = "hash_vishnuk"
v_phoneCollection = "hash_02495"

# Execute the instructions in the given order
createCollection(v_nameCollection)
createCollection(v_phoneCollection)

print("Initial Employee Count:", getEmpCount(v_nameCollection))

# Update the 'data_path' variable to the actual location of your employee data file
data_path = 'Employee.csv'
indexData(v_nameCollection, "Department", data_path)
indexData(v_phoneCollection, "Gender", data_path)

print("Employee Count After Indexing:", getEmpCount(v_nameCollection))

delEmpById(v_nameCollection, "E02003")

print("Employee Count After Deletion:", int(getEmpCount(v_nameCollection)))

print("Search by Department 'IT':", searchByColumn(v_nameCollection, "Department", "IT"))
print("Search by Gender 'Male':", searchByColumn(v_nameCollection, "Gender", "Male"))
print("Search by Department 'IT' in Phone Collection:", searchByColumn(v_phoneCollection, "Department", "IT"))

print("Department Facet in Name Collection:", getDepFacet(v_nameCollection))
print("Department Facet in Phone Collection:", getDepFacet(v_phoneCollection))