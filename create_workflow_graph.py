from py2neo import Graph, Node, Relationship
import argparse

import time
import extract_tools
import csv
import pandas as pd


class WorkflowGraphDatabase:

    def __init__(self, url, username, password):
        """ Init method. """
        self.graph = Graph(url, user=username, password=password)
        self.tool_node_name = "Tool"
        self.format_id_node_name = "Format"
        self.tool_output_relation_name = "OUTPUT"
        self.tool_input_relation_name = "INPUT"
        self.input_output_relation_name = "COMPATIBLE"

    def read_tool_connections(self, file_path):
        """
        Extract tool connections from tabular file
        """
        s_time = time.time()
        with open(file_path, 'rt') as workflow_connections_file:
            df_workflow_connections = pd.read_csv(workflow_connections_file, sep=",")
            print("Creating new database")
            transaction = self.graph.begin()
            for index, row in df_workflow_connections.iterrows():
                in_tool = row['in_tool']
                out_tool = row['out_tool']
                tool_input = row['tool_input']
                tool_output = row['tool_output']

                graph_components = {
                    "in_tool": in_tool,
                    "out_tool": out_tool,
                    "tool_input": tool_input,
                    "tool_output": tool_output
                }
                self.create_graph_records(graph_components)
                print("Record %d added" % (index + 1))
                if (index % 100) == 0:
                    transaction.commit()
                    transaction = self.graph.begin()
        e_time = time.time()
        print("Time elapsed in creating database: %d seconds" % int(e_time - s_time))
        
    def create_graph_records(self, graph_components):
        """
        Create nodes and relations for the database
        """
        tool_node_input = Node(self.tool_node_name, name=graph_components["in_tool"])
        tool_node_output = Node(self.tool_node_name, name=graph_components["out_tool"])
        format_node_input = Node(self.format_id_node_name, name=graph_components["tool_input"])
        format_node_output = Node(self.format_id_node_name, name=graph_components["tool_output"])

        relation_output = Relationship(tool_node_input, self.tool_output_relation_name, format_node_output)
        relation_compatible = Relationship(format_node_output, self.input_output_relation_name, format_node_input)
        relation_input = Relationship(format_node_input, self.tool_input_relation_name, tool_node_output)

        self.graph.merge(relation_output, "Tool_Format", "name")
        self.graph.merge(relation_compatible, "Compatible", "name")
        self.graph.merge(relation_input, "Tool_Format", "name")

    def create_graph_bulk_merge(self, file_name):
        """
        Create graph database with bulk import
        """
        # To make this query work, copy the csv file to /var/lib/neo4j/import/ and just pass the file name for the argument 'wf'
        query = "LOAD CSV WITH HEADERS FROM 'file:///" + file_name + "' AS tool_connections "
        query += "WITH tool_connections "
        query += "MERGE (in_tool: Tool {name: tool_connections.in_tool}) "
        query += "MERGE (out_tool: Tool {name: tool_connections.out_tool}) "
        query += "MERGE (tool_output: Format {name: tool_connections.tool_output}) "
        query += "MERGE (tool_input: Format {name: tool_connections.tool_input}) "
        query += "MERGE (in_tool) -[:OUTPUT]-> (tool_output) "
        query += "MERGE (tool_output) -[:COMPATIBLE]-> (tool_input) "
        query += "MERGE (tool_input) -[:INPUT]-> (out_tool) "

        print("Creating database in bulk...")
        s_time = time.time()
        self.graph.run(query)
        e_time = time.time()
        print("Time elapsed in creating database: %d seconds" % int(e_time - s_time))

    def create_index(self):
        self.graph.schema.create_index("Tool", "name")

    def fetch_records(self):
        print("Fetching records...")
        print()
        s_time = time.time()
        i_name = "toolshed.g2.bx.psu.edu/repos/pjbriggs/trimmomatic/trimmomatic/0.32.2"
        o_name = "bowtie2" #toolshed.g2.bx.psu.edu/repos/devteam/bowtie2/bowtie2/2.2.6.2
        get_all_nodes_query = "MATCH (n) RETURN n"
        all_nodes = self.graph.run(get_all_nodes_query).data()
        #print(all_nodes)
        print("Number of all nodes: %d" % len(all_nodes))
        print()
        query1 = "MATCH (a:Tool {name: {name_a}}) - [:OUTPUT] -> (b:Tool {name: {name_b}}) RETURN a, b"
        query2 = "MATCH (a:Tool { name: {name_a}}), (b:Tool { name: {name_b}}), p = shortestPath((a)-[*]-(b)) RETURN p"
        # get the shortest path between two nodes having certain minimum length 
        query3 = "MATCH (a:Tool { name: {name_a}}), (b:Tool { name: {name_b}}), p = shortestPath((a)-[*]-(b)) WHERE length(p) > 1 RETURN p"
        # get all paths/relations
        query4 = "MATCH (a:Tool {name: {name_a}}) - [r*..10] -> (b:Tool {name: {name_b}}) RETURN r LIMIT 10"
        fetch = self.graph.run(query4, name_a=i_name, name_b=o_name).data()
        # get next tool for any tool
        query5 = "MATCH (a:Tool {name: {name_a}}) - [r*..3] -> (b:Tool) RETURN COLLECT(distinct b.name) as predicted_tools"
        #fetch = self.graph.run(query5, name_a=i_name).data()
        for path in fetch:
            print(path)
            print()
        e_time = time.time()
        print("Time elapsed in fetching records: %d seconds" % int(e_time - s_time))


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-url", "--url", required=True, help="Neo4j server")
    arg_parser.add_argument("-un", "--user_name", required=True, help="User name")
    arg_parser.add_argument("-pass", "--password", required=True, help="Password")
    arg_parser.add_argument("-cd", "--create_database", required=True, help="Create a new database or not")
    arg_parser.add_argument("-wf", "--workflow_file", required=True, help="Workflow file")
    args = vars(arg_parser.parse_args())
    url = args["url"]
    username = args["user_name"]
    password = args["password"]
    create_db = args["create_database"]
    workflow_file = args["workflow_file"]
    # connect to neo4j database
    graph_db = WorkflowGraphDatabase(url, username, password)
    # create a database after deleting the existing records
    if create_db == "true":
        n = graph_db.graph.delete_all()
        assert n == None
        graph_db.create_graph_bulk_merge(workflow_file)
    # run queries against database
    graph_db.fetch_records()
