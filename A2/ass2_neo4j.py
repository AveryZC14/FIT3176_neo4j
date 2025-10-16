### WRITE YOUR STUDENT ID, MONASH EMAIL ADDRESS AND FULL NAME BELOW
# STUDENT ID:
# FULL NAME:
# MONASH EMAIL:

#### DO NOT IMPORT ANY OTHER LIBRARIES
import json
from neo4j import GraphDatabase
#### DO NOT IMPORT ANY OTHER LIBRARIES

# DO NOT DELETE THE CODE BELOW
# Neo4j connection details
uri = "bolt://localhost:7687"
username = ""  # Provide actual username if set. However, reset the username to an empty string before submitting
password = ""  # Provide actual password if set. However, reset the password to an empty string before submitting.

# Initialize driver with or without authentication based on whether credentials are provided
if username and password:
    driver = GraphDatabase.driver(uri, auth=(username, password))
else:
    driver = GraphDatabase.driver(uri)

# Global session variable for executing tasks
session = None

# Function to delete all nodes and relationships from the database
def delete_all_data():
    delete_query = """
    MATCH (n)
    DETACH DELETE n
    """
    session.run(delete_query)

# DO NOT DELETE THE CODE ABOVE

def print_results(result):
    print("#" * 40)
    print("               RESULTS")
    print("#" * 40)
    c = 0
    for record in result:
        c+=1
        # print(c)
        print("-" * 40)
        for key, value in record.items():
            print(f"{key}: {value}")    
    print("-" * 40)

# Task 1: Function to recreate the database by deleting all data and populating it with new data
def recreate_database():
    # Load data from the ./listings_cleaned.json file
    with open('./airbnb_listings.json', 'r') as file:
        data = json.load(file)

    # First, delete all existing data
    delete_all_data()

    # Write your code to create the database here.
    # Feel free to create new functions that this function calls to recreate the database
    for listing in data[:]:
        process_listing(listing)

def process_listing(listing):
    #create the listing
    #TODO: LISTING LOCATION
    session.run("""
        MERGE (l:Listing {id:$id, name:$name, price:$price})
        MERGE (n:Neighbourhood {name:$nbh})
        MERGE (l) -[:LOCATED_IN]-> (n)
        MERGE (h:Host {id:$h_id, name: $h_name, location:$h_location, is_superhost:$h_super})
        MERGE (h) -[:HOSTS]-> (l)
    """, id=listing["id"], name=listing["name"], 
    price=listing["price"], nbh = listing["neighbourhood"],
    h_id=listing["host_id"], h_name=listing["host_name"],
    h_location=listing["host_location"] or "", h_super = (listing["host_is_superhost"] == "t") )

    amenities = []
    if "amenities" in listing:
        amenities = listing["amenities"]
    for amenity in amenities:
        session.run("""
            MATCH (l:Listing {id: $l_id})
            MERGE (a:Amenity {name: $amenity})
            MERGE (l) -[:HAS_AMENITY]-> (a)
        """, l_id = listing["id"], amenity = amenity)
    


def task2():

    # Find the neighbourhood with the highest number of listings, with ties broken by alphabetical order of the neighbourhood name.
    # Print the most expensive listing in the neighbourhood
    res = session.run("""
        MATCH (l:Listing) -[:LOCATED_IN]-> (n:Neighbourhood)
        return n.name, COUNT(l)
    """)
    print_results(res)
    # pass

def task3(neighbourhood, distance_km):
    # Find the most expensive listing in the specified neighbourhood, with ties broken by listing_id (smaller is better).
    # Find other listingsss within distance_km of the most expensive listing and print the 3-lowest priced ones, with ties broken by distance
    pass


# Main function that controls the flow of the script
def main():
    # Establish a Neo4j session
    global session
    with driver.session() as session_obj:
        session = session_obj

        # Uncomment the following line to recreate the database from scratch
        # recreate_database()
        task2()
        #task3("Melbourne", 5)

# Run the main function
if __name__ == "__main__":
    main()

# Close the driver after use
driver.close()
