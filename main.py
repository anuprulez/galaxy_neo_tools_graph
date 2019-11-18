from py2neo import Graph, Node, Relationship
import argparse

import extract_tools


class ToolGraphDatabase:

    def __init__(self, url, username, password):
        """ Init method. """
        self.graph = Graph(url, user=username, password=password)

    def create_records(self):
        transaction = self.graph.begin()
        node_a = Node("Person", name="Alice", place="Sweden")
        transaction.create(node_a)
        node_b = Node("Person", name="Bob", place="Sweden")
        relation = Relationship(node_a, "KNOWS", node_b)
        transaction.create(relation)
        transaction.commit()

    def fetch_records(self):
        fetch = self.graph.run("MATCH (a:Person {place: {place}}) RETURN a.name, a.place", place="Sweden").data()
        print(fetch)


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-url", "--url", required=True, help="Neo4j server")
    arg_parser.add_argument("-un", "--user_name", required=True, help="User name")
    arg_parser.add_argument("-pass", "--password", required=True, help="Password")
    arg_parser.add_argument("-tf", "--tools_file", required=True, help="Tools file")
    args = vars(arg_parser.parse_args())
    # extract information for tools
    tools = extract_tools.ToolInfo(args["tools_file"])
    tool_ids = tools.read_tools()
    tools.fetch_tool(tool_ids)


    '''url = args["url"]
    username = args["user_name"]
    password = args["password"]
    graph_db = ToolGraphDatabase(url, username, password)
    #graph_db.create_records()
    graph_db.fetch_records()'''
