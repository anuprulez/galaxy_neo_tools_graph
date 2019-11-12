from py2neo import Graph, Node, Relationship
import argparse

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
        with open(file_path, 'rt') as workflow_connections_file:
            df_workflow_connections = pd.read_csv(workflow_connections_file, sep="\t")
            transaction = self.graph.begin()
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
                        "tool_input": row['tool_outputs'],
                        "tool_output": row['tool_outputs']
                    }
                    self.create_graph_records(graph_components, transaction)
                    print("Record %d added" % (index + 1))
            transaction.commit()

    def create_graph_records(self, graph_components, transaction):
        tool_node_input = Node("Tool", name=graph_components["in_tool"])
        tool_node_output = Node("Tool", name=graph_components["out_tool"])
        format_node_input = Node("Format", name=graph_components["tool_input"])
        format_node_output = Node("Format", name=graph_components["tool_output"])
        relation_output = Relationship(tool_node_input, self.tool_output_relation_name, format_node_output)
        relation_compatible = Relationship(format_node_output, self.input_output_relation_name, format_node_input)
        relation_input = Relationship(tool_node_input, self.tool_output_relation_name, tool_node_output)

        self.graph.merge(relation_output, "Tool_Format", "name")
        self.graph.merge(relation_compatible, "Compatible", "name")
        self.graph.merge(relation_input, "Tool_Format", "name")

    def fetch_records(self):
        i_name = "cat1"
        o_name = "barchart_gnuplot"
        fetch = self.graph.run("MATCH (a:Tool {name: {name_a}}) - [:OUTPUT] -> (b:Tool {name: {name_b}}) RETURN a, b", name_a=i_name, name_b=o_name).data()
        print(fetch)


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-url", "--url", required=True, help="Neo4j server")
    arg_parser.add_argument("-un", "--user_name", required=True, help="User name")
    arg_parser.add_argument("-pass", "--password", required=True, help="Password")
    arg_parser.add_argument("-wf", "--workflow_file", required=True, help="Workflow file")
    args = vars(arg_parser.parse_args())
    url = args["url"]
    username = args["user_name"]
    password = args["password"]
    # extract information for tools
    graph_db = WorkflowGraphDatabase(url, username, password)
    #graph_db.read_tool_connections(args["workflow_file"])
    graph_db.fetch_records()

