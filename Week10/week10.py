import pandas as pd
from neo4j import GraphDatabase

# Neo4j connection details
uri = "bolt://localhost:7687"
username = ""  # Provide actual username if set
password = ""  # Provide actual password if set
driver = GraphDatabase.driver(uri, auth=(username, password))

# Close the Neo4j driver
def close_driver(driver):
    driver.close()

# Function to delete all nodes
def delete_all_nodes(driver):
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

# Function to create or merge Year node
def create_year_node(driver, year):
    with driver.session() as session:
        session.run("MERGE (y:Year {year: $year})", year=year)

# Function to create or merge Month node and relationship with Year
def create_month_node(driver, month, year):
    with driver.session() as session:
        session.run("""
            MATCH (y:Year {year: $year})
            MERGE (m:Month {month: $month, year: $year})
            MERGE (m)-[:BELONGS_TO]->(y)
        """, month=month, year=year)

# Function to create or merge State and City nodes (To be implemented)
def create_city_state_nodes(driver, city, state):
    with driver.session() as session:
        session.run("""
            MERGE (state: State {name:$state})
            MERGE (c:City {name:$city, state:$state})-[r:LOCATED_IN]->(state)
        """, city=city, state=state)

# Function to create or merge Organisation node and relationship with City (To be implemented)
def create_organisation_city_relationship(driver, organisation, city, state):
    with driver.session() as session:
        session.run("""
            MATCH (city:City {name:$city, state:$state})
            MERGE (o:Organisation {name:$org})-[l:LOCATED_IN]->(city)
        """, org=organisation, state=state, city=city)

# Function to create Article node (To be implemented)
def create_article_node(driver, article_id, title, date, url, keywords, summary):
    summary = str(summary) if summary is not None else ""
    keywords = str(keywords)
    with driver.session() as session:
        session.run("""
            MERGE (a:Article {id:$id, title:$title, date:$date, url:$url, keywords:$keywords, summary:$summary})
        """, id=article_id, date=date, title=title, url=url, keywords=keywords, summary=summary)

# Function to create Article-Month relationship (To be implemented)
def create_article_month_relationship(driver, article_id, month, year):
    with driver.session() as session:
        session.run("""
            MATCH (article:Article {id:$art_id}),
                    (month:Month {month:$month, year:$year})
            MERGE (article)-[:REPORTED_IN]->(month)
        """, art_id=article_id, month=month, year=year)

# Function to create Article-Organisation relationship (To be implemented)
def create_article_organisation_relationship(driver, article_id, organisation):
    with driver.session() as session:
        session.run("""
            MATCH (article:Article {id:$art_id}),
                    (org:Organisation {name:$org})
            MERGE (article)-[:REPORTED_BY]->(org)
        """, art_id=article_id, org=organisation)

# can_cats = []
# Function to create Category nodes and relationships (To be implemented)
def create_category_nodes_relationship(driver, article_id, categories):
    cats = map((lambda x:x.strip()),categories.split(","))
    with driver.session() as session:
        for cat in cats:
            # if cat not in can_cats:
            #     can_cats.append(cat)
            #     print(cat)
            session.run("""
                MATCH (article:Article {id:$art_id})
                MERGE (cat:Category {name: $cat})
                CREATE (article)-[:BELONGS_TO]->(cat)
            """, art_id=article_id, cat=cat)

# Function to create the graph for each row in the DataFrame
def create_graph(driver, df):
    i=0
    for index, row in df.iterrows():
        create_year_node(driver, int(row['Year']))
        create_month_node(driver, row['Month'], int(row['Year']))
        create_city_state_nodes(driver, row['City'], row['State'])
        create_organisation_city_relationship(driver, row['Organisation'], row['City'], row['State'])
        create_article_node(driver, row['ID'], row['Title'], row['Date'], row['URL'], row['Keywords'], row['Summary'])
        create_article_month_relationship(driver, row['ID'], row['Month'], int(row['Year']))
        create_article_organisation_relationship(driver, row['ID'], row['Organisation'])
        create_category_nodes_relationship(driver, row['ID'], row['Category'])
		# the following block will print a message after every 100 rows have been processed
        i=i+1
        if(i%100==0):
            print(i,"rows processed")


# Load and clean the CSV file
def load_and_clean_data(file_path):
    df = pd.read_csv(
        file_path,
        encoding='utf-8',  # Ensure the encoding matches your file
        engine='python'  # Use the Python engine for more flexibility with line endings
    )

    # Parse the Date column to datetime, considering the format 'dd-Mon-yy'
    df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%y', errors='coerce')

    # Drop rows where the 'Date' column could not be parsed
    df = df.dropna(subset=['Date'])

    # Extract Year and Month from the Date column
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.strftime('%B')

    # Ensure there are no NaN values in Year and Month
    df = df.dropna(subset=['Year', 'Month'])

    return df

# Create the graph database
def create_graph_db(driver, df):
    delete_all_nodes(driver)
    create_graph(driver, df)

def sample_task(session):
    # Find all articles reported by 'The Sun Herald'
    result = session.run("""
        MATCH (a:Article)-[:REPORTED_BY]->(o:Organisation {name: 'The Sun Herald'})
        RETURN a.title, a.url, a.date
    """)
    return result

#tasks innit
def task7(session):
    result = session.run("""
        MATCH 
            (a:Article)-[:BELONGS_TO]->(c:Category {name: "Anti-Muslim"}),
            (a)-[:REPORTED_IN]->(m:Month {month: "March", year: 2017})
        RETURN a.title as Title
        """)
    # print(len(result))
    return result


def task8(session):
    result = session.run("""
        MATCH 
            (o:Organisation)-[:LOCATED_IN]->(c:City {state: "NY"}),
            (a:Article)-[:REPORTED_BY]->(o)
        RETURN count(a)
        """)
    # print(len(result))
    return result

def task9(session):
    result = session.run("""
        MATCH 
            (o:Organisation)-[:LOCATED_IN]->(c:City {state: "WA"}),
            (a:Article)-[:REPORTED_BY]->(o),
            (a:Article)-[:BELONGS_TO]->(cat:Category {name: "Antisemitism"})
        RETURN a.title as Title, a.url as URL
        """)
    # print(len(result))
    return result

def task10(session):
    result = session.run("""
        MATCH 
            (o:Organisation)-[:LOCATED_IN]->(c:City {name: "New York"}),
            (a:Article)-[:REPORTED_BY]->(o),
            (a:Article)-[:BELONGS_TO]->(cat:Category)
        WHERE cat.name in ["Racism", "Anti-LGBTQ+"] 
        RETURN count(a)
        """)
    # print(len(result))
    return result


def task11(session):
    result = session.run("""
        MATCH 
            (a:Article)-[:REPORTED_BY]->(o:Organisation),
            (a:Article)-[:REPORTED_IN]->(m:Month {year: 2017})
        RETURN o.name as Organisation, count(a) as total_articles
        ORDER BY count(a) DESC
        LIMIT 5
        """)
    # print(len(result))
    return result

def task12(session):
    result = session.run("""
        MATCH 
            (a:Article)-[:BELONGS_TO]->(c:Category)
        WHERE NOT c.name = "Other" 
        RETURN c.name as Category, count(a) as total_articles
        ORDER BY count(a) DESC
        LIMIT 3
        """)
    # print(len(result))
    return result

def task13(session):
    result = session.run("""
        MATCH 
            (a:Article)-[:REPORTED_IN]->(m:Month {year: 2017}),
            (a)-[:BELONGS_TO]->(c:Category {name:"Antisemitism"})
        RETURN m.month as Month, count(a) as total_articles
        ORDER BY count(a) DESC
        """)
    # print(len(result))
    return result

# Main function
def main():
    file_path = './HateCrimeReports.csv'
    # df = load_and_clean_data(file_path)

    # print("Printing first 5 rows of DataFrame to see if the data is read correctly:")
    # print(df.head())

    # create_graph_db(driver, df)

    run_queries()
    close_driver(driver)

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

def run_queries():
    with driver.session() as session:
        # print_results(sample_task(session))
        print_results(task7(session))
        print_results(task8(session))
        print_results(task9(session))
        print_results(task10(session))
        print_results(task11(session))
        print_results(task12(session))
        print_results(task13(session))

if __name__ == "__main__":
    main()
        