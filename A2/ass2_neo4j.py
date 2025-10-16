### WRITE YOUR STUDENT ID, MONASH EMAIL ADDRESS AND FULL NAME BELOW
# STUDENT ID: 33905444
# FULL NAME: Avery Cheng
# MONASH EMAIL: ache0138@student.monash.edu

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
        MERGE (l:Listing {id:$id, name:$name, price:$price, location: point({longitude: $long, latitude: $lat}) })
        MERGE (n:Neighbourhood {name:$nbh})
        MERGE (l) -[:LOCATED_IN]-> (n)
        MERGE (h:Host {id:$h_id, name: $h_name, location:$h_location, is_superhost:$h_super})
        MERGE (h) -[:HOSTS]-> (l)
    """, id = listing["id"], 
    name = listing["name"], 
    price = listing["price"],
    long = listing["longitude"],
    lat = listing["latitude"],
    nbh = listing["neighbourhood"],
    h_id = listing["host_id"], h_name = listing["host_name"],
    h_location = listing["host_location"] or "", 
    h_super = (listing["host_is_superhost"] == "t"))

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
        MATCH (l1:Listing) -[:LOCATED_IN]-> (n:Neighbourhood)
        //keep popular_n as a variable for the best
        WITH n as popular_n, COUNT(l1) as n_count
        ORDER BY COUNT(l1) DESC, n.name ASC
        LIMIT 1
        
        MATCH (l2:Listing) -[:LOCATED_IN]-> (popular_n)
        RETURN l2.id as l_id, 
            l2.name as l_name, 
            l2.price as l_price, 
            popular_n.name as n_name, 
            n_count
        ORDER BY l2.price DESC
        LIMIT 1
        
    """)
    
    #single() gets just one record out of a result, dict() turns the items of that record into a dictionary
    result = res.single()
    result_dict = dict(result.items())
    
    listing_id = result_dict["l_id"]
    listing_name = result_dict["l_name"]
    listing_price = result_dict["l_price"]
    neighbourhood_name = result_dict["n_name"]
    neighbourhood_listing_count = result_dict["n_count"]
    
    print(
        f"""Neighbourhood with the highest number of listings: {neighbourhood_name} (Total Listings: {neighbourhood_listing_count})
Most expensive listing in {neighbourhood_name}:
Listing: ID: {listing_id}, Name: {listing_name}, Price: ${listing_price}
        """
    )

def task3(neighbourhood, distance_km):
    # Find the most expensive listing in the specified neighbourhood, with ties broken by listing_id (smaller is better).
    # Find other listingsss within distance_km of the most expensive listing and print the 3-lowest priced ones, with ties broken by distance
    expensive_res = session.run("""
        MATCH (l1:Listing) -[:LOCATED_IN]-> (n:Neighbourhood {name:$nbh}),
            (h1:Host) -[:HOSTS]-> (l1)
        return l1.name as l1_name, 
            l1.id as l1_id, 
            l1.price as l1_price, 
            h1.name as h1_name
        ORDER BY l1.price DESC, l1.id ASC
        LIMIT 1
    """,
    nbh = neighbourhood,
    max_dist = distance_km*1000)
    
    exp_result = expensive_res.single()
    # print(exp_result)
    
    if exp_result == None:
        print(f"No listings found in the neighbourhood ’{neighbourhood}’")
        return
    exp_listing = dict(exp_result.items())
    
    l1_id = exp_listing["l1_id"]
    l1_name = exp_listing["l1_name"]
    l1_price = exp_listing["l1_price"]
    l1_host = exp_listing["h1_name"]
    print(f"Most expensive listing in {neighbourhood}: ID: {l1_id}, Name: {l1_name} (Price: ${l1_price}, Host: {l1_host})")
        
        
    others_res = session.run("""
        MATCH (l1:Listing {id:$exp_id})
        WITH l1
        LIMIT 1
        
        MATCH (l2:Listing), (h2:Host) -[:HOSTS]-> (l2)
        WHERE point.distance(l1.location, l2.location) < $max_dist
        AND NOT l1.id = l2.id
        RETURN
            l2.name as l2_name,
            l2.id as l2_id, 
            l2.price as l2_price, 
            h2.name as h2_name,
            round(point.distance(l1.location, l2.location)/1000, 2) as dist
        ORDER BY l2.price ASC, point.distance(l1.location, l2.location) ASC, l2.id ASC
        LIMIT 3
    """,
    exp_id = l1_id,
    nbh = neighbourhood,
    max_dist = distance_km*1000)
    
    if (others_res.peek()) == None:
        print(f"No listings found within {distance_km} km of ’{l1_name}’.")
    else:
        print(f"Listings within {distance_km} km of ’{l1_name}’ with the lowest prices:")
        for result in (others_res):
            result_dict = dict(result)
            l2_id = result_dict["l2_id"]
            l2_name = result_dict["l2_name"]
            l2_price = result_dict["l2_price"]
            l2_host = result_dict["h2_name"]
            dist = result_dict["dist"]
            print(f"Listing: ID: {l2_id}, Name: {l2_name}, Price: ${l2_price}, Host: {l2_host}, Distance: {dist} km")

# Main function that controls the flow of the script
def main():
    # Establish a Neo4j session
    global session
    with driver.session() as session_obj:
        session = session_obj

        # Uncomment the following line to recreate the database from scratch
        # recreate_database()
        # task2()
        task3("Melbourne", 10)
        print("\n")
        task3("Melbourne", 100)
        print("\n")
        task3("Monash", 10)
        print("\n")
        task3("Moreland",10)
        print("\n")
        task3("Casey", 10)
        print("\n")
        task3("Clayton", 10)
        

# Run the main function
if __name__ == "__main__":
    main()

# Close the driver after use
driver.close()
