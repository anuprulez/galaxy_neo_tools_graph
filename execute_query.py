from neo4j import GraphDatabase


uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

def run_query(tx, name):
    for record in tx.run("MATCH (people:Person {name: {name}}) RETURN people.name LIMIT 10", name=name):
        print(record["people.name"])

with driver.session() as session:
    session.read_transaction(run_query, "Tom Hanks")
