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
        s_time = time.time()
        with open(file_path, 'rt') as workflow_connections_file:
            df_workflow_connections = pd.read_csv(workflow_connections_file, sep="\t")
            print("Creating new database")
            transaction = self.graph.begin(autocommit=True)
            for index, row in df_workflow_connections.iterrows():
                row['in_tool'] = row['in_tool'].strip()
                row['out_tool'] = row['out_tool'].strip()
                row['tool_inputs'] = row['tool_inputs'].strip()
                row['tool_outputs'] = row['tool_outputs'].strip()

                if len(row['in_tool']) > 0 \
                    and len(row['out_tool']) > 0 \
                    and len(row['tool_inputs']) > 0 \
                    and len(row['tool_outputs']) > 0:
                    graph_components = {
                        "in_tool": row['in_tool'],
                        "out_tool": row['out_tool'],
                        "tool_input": row['tool_inputs'],
                        "tool_output": row['tool_outputs']
                    }
                    self.create_graph_records(graph_components, transaction)
                    print("Record %d added" % (index + 1))
            #transaction.commit()
        e_time = time.time()
        print("Time elapsed in creating database: %d seconds" % int(e_time - s_time))

    def create_graph_records(self, graph_components, transaction):
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
        
    def create_index(self):
        self.graph.schema.create_index("Tool", "name")

    def fetch_records(self):
        print("Fetching records...")
        print()
        s_time = time.time()
        i_name = "cat1"
        o_name = "bamCoverage_deepTools"
        get_all_nodes_query = "MATCH (n) RETURN n"
        delete_all_nodes_query = "MATCH (n) DETACH DELETE n RETURN n"
        query1 = "MATCH (a:Tool {name: {name_a}}) - [:OUTPUT] -> (b:Tool {name: {name_b}}) RETURN a, b"
        query2 = "MATCH (a:Tool { name: {name_a}}), (b:Tool { name: {name_b}}), p = shortestPath((a)-[*]-(b)) RETURN p"
        # get the shortest path between two nodes having certain minimum length 
        query3 = "MATCH (a:Tool { name: {name_a}}), (b:Tool { name: {name_b}}), p = shortestPath((a)-[*]-(b)) WHERE length(p) > 1 RETURN p"
        # get all paths/relations
        query4 = "MATCH (a:Tool {name: {name_a}}) - [r*] -> (b:Tool {name: {name_b}}) RETURN r LIMIT 5"
        # get next tool for any tool
        query5 = "MATCH (a:Tool {name: {name_a}}) - [r*..3] -> (b:Tool) RETURN COLLECT(distinct b.name) as predicted_tools LIMIT 20"
        #fetch = self.graph.run(query5, name_a=i_name).data()
        fetch = self.graph.run(query4, name_a=i_name, name_b=o_name).data()
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
        graph_db.read_tool_connections(workflow_file)
        graph_db.create_index()
    # run queries against database
    graph_db.fetch_records()
