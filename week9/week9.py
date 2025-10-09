from neo4j import GraphDatabase

# Define file paths
countries_file = 'countries.txt'
years_file = 'years.txt'
gender_pay_gap_file = 'gender_pay_gap.txt'

# Neo4j connection details
uri = "bolt://localhost:7687"
username = ""  # Provide actual username if set
password = ""  # Provide actual password if set
driver = GraphDatabase.driver(uri, auth=(username, password))

def close_driver(driver):
    driver.close()

def load_data(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

def delete_all_nodes(driver):
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

def create_country_nodes(driver, countries):
    with driver.session() as session:
        for country in countries:
            code, name = country.split(', ')
            session.run("CREATE (c:Country {code: $code, name: $name})", code=code, name=name)

def create_year_nodes(driver, years):
    with driver.session() as session:
        for year in years:
            session.run("CREATE (y:Year {year: $year})", year=int(year))

def create_relationships(driver, relationships):
    with driver.session() as session:
        for relationship in relationships:
            code, year, gap = relationship.split(', ')
            session.run("""
                MATCH (c:Country {code: $code}), (y:Year {year: $year})
                CREATE (c)-[:HAS_GENDER_PAY_GAP {gap: $gap}]->(y)
                """, code=code, year=int(year), gap=float(gap))


# (Re)Create nodes and relationships in Neo4j
def recreate_graph(driver):
    delete_all_nodes(driver)    
    create_country_nodes(driver, countries)
    create_year_nodes(driver, years)
    create_relationships(driver, relationships)

# Retrieve the gender pay gap data for Australia in all years.
def task4(driver):
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Country {code: 'AUS'})-[r:HAS_GENDER_PAY_GAP]->(y:Year)
            RETURN c.name AS Country, y.year AS Year, r.gap AS GenderPayGap
            ORDER BY y.year;
        """)
        for record in result:
            print(f"Country: {record['Country']}, Year: {record['Year']}, Gender Pay Gap: {record['GenderPayGap']}%")

# Retrieve all countries with a gender pay gap greater than 15% in the year 2022.
def task5(driver):
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Country)-[r:HAS_GENDER_PAY_GAP]->(y:Year {year: 2022})
            WHERE r.gap > 15
            RETURN c.name AS Country, y.year AS Year, r.gap AS GenderPayGap
            ORDER BY r.gap DESC;
        """)
        for record in result:
            print(f"Country: {record['Country']}, Year: {record['Year']}, Gender Pay Gap: {record['GenderPayGap']}%")

# Update the gender pay gap data for Australia in the year 2022 to 18%.
def task6(driver):
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Country {code: 'AUS'})-[r:HAS_GENDER_PAY_GAP]->(y:Year {year: 2022})
            SET r.gap = 18
            RETURN c.name AS Country, y.year AS Year, r.gap AS UpdatedGenderPayGap;
        """)
        for record in result:
            print(f"Country: {record['Country']}, Year: {record['Year']}, Updated Gender Pay Gap: {record['UpdatedGenderPayGap']}%")

# Delete the gender pay gap data for Australia in the year 2022.
def task7(driver):
    with driver.session() as session:
        session.run("""
            MATCH (c:Country {code: 'AUS'})-[r:HAS_GENDER_PAY_GAP]->(y:Year {year: 2022})
            DELETE r;
        """)
    print("Deleted the gender pay gap data for Australia in 2022.")

# Display the countries and their gender pay gap for the year 2022 in descending order of pay gap.
def task8(driver):
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Country)-[r:HAS_GENDER_PAY_GAP]->(y:Year {year: 2022})
            RETURN c.name AS Country, r.gap AS GenderPayGap
            ORDER BY r.gap DESC;
        """)
        for record in result:
            print(f"Country: {record['Country']}, Gender Pay Gap: {record['GenderPayGap']}%")

# Find data where countries had a negative gender pay gap in any year.
def task9(driver):
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Country)-[r:HAS_GENDER_PAY_GAP]->(y:Year)
            WHERE r.gap < 0
            RETURN c.name AS Country, y.year AS Year, r.gap AS GenderPayGap
            ORDER BY r.gap ASC;
        """)
        if result.peek() is None:
            print("There is no country with negative gender pay gap in any year")
        else:
            for record in result:
                print(f"Country: {record['Country']}, Year: {record['Year']}, Gender Pay Gap: {record['GenderPayGap']}%")

# Display the gender pay gap of Australia in each year, sorted by descending order of gender pay gap.
def task10(driver):
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Country {code: 'AUS'})-[r:HAS_GENDER_PAY_GAP]->(y:Year)
            RETURN c.name AS Country, y.year AS Year, r.gap AS GenderPayGap
            ORDER BY r.gap DESC;
        """)
        for record in result:
            print(f"Country: {record['Country']}, Year: {record['Year']}, Gender Pay Gap: {record['GenderPayGap']}%")

# Count the number of years each country has gender pay gap data for.
def task11(driver):
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Country)-[r:HAS_GENDER_PAY_GAP]->(y:Year)
            RETURN c.name AS Country, COUNT(y) AS NumberOfYears
            ORDER BY NumberOfYears DESC;
        """)
        for record in result:
            print(f"Country: {record['Country']}, Number of Years: {record['NumberOfYears']}")


# Load data from files
countries = load_data(countries_file)
years = load_data(years_file)
relationships = load_data(gender_pay_gap_file)


def main():
    # Execute all tasks
    recreate_graph(driver)
    task5(driver)
    task6(driver)
    task7(driver)
    recreate_graph(driver)
    task8(driver)
    task9(driver)
    task10(driver)
    task11(driver)

    # Close the driver connection
    close_driver(driver)
  

if __name__ == "__main__":
    main()

