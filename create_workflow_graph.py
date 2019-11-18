import argparse
import os
import time


from py2neo import Graph, Node, Relationship


class WorkflowGraphDatabase:

    def __init__(self, url, username, password):
        """ Init method. """
        self.graph = Graph(url, user=username, password=password)
        self.components = {
            'Nodes': {
                'Tool': 'Tool',
                'Version': 'Version',
                'ToolOutput': 'ToolOutput',
                'ToolInput': 'ToolInput',
                'WorkflowConnection': 'WorkflowConnection',
                'Workflow': 'Workflow',
                'Datatype': 'Datatype',
                'EDAMFormat': 'EDAMFormat',
            },
            'Relationships': {
                'Tool_to_Version': 'IS_VERSION_OF',
                'Version_to_ToolOutput': 'GENERATES_OUTPUT',
                'Version_to_ToolInput': 'TAKES_INPUT',
                'WorkflowConnection_to_ToolOutput': 'CONNECTS_OUTPUT',
                'WorkflowConnection_to_ToolInput': 'TO_INPUT',
                'Workflow_to_WorkflowConnection': 'DESCRIBES_CONNECTION',
                'ToolOutput_to_Datatype': 'HAS_DATATYPE',
                'ToolInput_to_Datatype': 'HAS_DATATYPE',
                'Datatype_to_EDAMFormat': 'IS_OF_FORMAT',
            }
        }
        self.tool_node = "Tool"
        self.version_node = "Version"
        self.format_node = "Format"
        self.tool_version_relation = "IS_VERSION_OF"
        self.version_output_format_relation = "V_OUTPUT_FORMAT"
        self.output_input_format_relation = "COMPATIBLE"
        self.input_format_version_relation = "INPUT_FORMAT_V"
        self.version_tool_relation = "V_TOOL"

    def create_graph_bulk_merge(self, file_name):
        """
        Create graph database with bulk import
        """
        # To make this query work, copy the csv file to /var/lib/neo4j/import/ and just pass the file name for the argument 'wf'        
        '''query = "LOAD CSV WITH HEADERS FROM 'file:///" + file_name + "' AS tool_connections "
        query += "WITH tool_connections "
        query += "MERGE (in_tool: Tool {name: tool_connections.in_tool}) "

        query += "WITH tool_connections "
        query += "MATCH (in_tool: Tool {name: tool_connections.in_tool}) MERGE (in_tool) -[:TOOL_V] ->(: Version {name: tool_connections.in_tool_version}) "

        query += "WITH tool_connections "
        query += "MATCH (: Tool {name: tool_connections.in_tool}) -[:TOOL_V] ->(tool_input_v: Version {name: tool_connections.in_tool_version}) "
        query += "MERGE (tool_input_v) -[:V_OUTPUT_FORMAT] ->(: Format {name: tool_connections.in_tool_output}) "

        query += "WITH tool_connections "
        query += "MATCH (: Tool {name: tool_connections.in_tool}) -[:TOOL_V] ->(: Version {name: tool_connections.in_tool_version}) -[:V_OUTPUT_FORMAT] ->(tool_output: Format {name: tool_connections.in_tool_output}) "
        query += "MERGE (tool_output) -[:COMPATIBLE] ->(: Format {name: tool_connections.out_tool_input}) " 

        query += "WITH tool_connections "
        query += "MATCH (:Tool {name: tool_connections.in_tool}) -[:TOOL_V] ->(:Version {name: tool_connections.in_tool_version}) -[:V_OUTPUT_FORMAT] ->(:Format {name: tool_connections.in_tool_output}) -[:COMPATIBLE] ->(tool_input: Format {name: tool_connections.out_tool_input}) "
        query += "MERGE (tool_input) -[:INPUT_FORMAT_V] ->(tool_output_v: Version {name: tool_connections.out_tool_version}) "
        
        query += "WITH tool_connections "
        query += "MATCH (:Tool {name: tool_connections.in_tool}) -[:TOOL_V] ->(:Version {name: tool_connections.in_tool_version}) -[:V_OUTPUT_FORMAT] ->(: Format {name: tool_connections.in_tool_output}) -[:COMPATIBLE] ->(:Format {name: tool_connections.out_tool_input}) -[:INPUT_FORMAT_V] ->(: Version {name: tool_connections.out_tool_version}) "

        query += "WITH tool_connections "
        query += "MATCH (: Tool {name: tool_connections.in_tool}) -[:TOOL_V] ->(: Version {name: tool_connections.in_tool_version}) -[:V_OUTPUT_FORMAT] ->(: Format {name: tool_connections.in_tool_output}) -[:COMPATIBLE] ->(: Format {name: tool_connections.out_tool_input}) -[:INPUT_FORMAT_V] ->(tool_output_v: Version {name: tool_connections.out_tool_version}) "
        query += "MERGE (tool_output_v) -[:V_TOOL] ->(out_tool: Tool {name: tool_connections.out_tool}) "
        
        print(query)'''
        
        wf_query = "LOAD CSV WITH HEADERS FROM 'file:///corrected_gxadmin_workflow_connections_88551.csv' AS tc "
        wf_query += "MATCH (in_tool: Tool {name: tc.in_tool}) MERGE (in_tool)<-[:IS_VERSION_OF]-(in_v: Version {name: tc.in_tool_version}) "
        wf_query += "WITH tc, in_tool, in_v "
        wf_query += "MATCH (out_tool: Tool {name: tc.out_tool}) MERGE (out_tool)<-[:IS_VERSION_OF]-(out_v:Version {name: tc.out_tool_version}) "
        wf_query += "WITH tc, in_tool, in_v, out_tool, out_v "
        wf_query += "MATCH (in_tool) <- [:IS_VERSION_OF] -(in_v) MERGE (in_v) -[:GENERATES_OUTPUT] ->(d_out: ToolOutput {name: tc.in_tool_output}) "
        wf_query += "WITH tc, out_tool, out_v "
        wf_query += "MATCH (out_tool) <- [:IS_VERSION_OF] -(out_v) MERGE (out_v) -[:TAKES_INPUT] ->(d_in: ToolInput {name: tc.out_tool_input}) "
        wf_query += "MERGE (d_out) -[:WORKFLOW_CONNECTION] -> (d_in)"

        print("Creating database in bulk...")
        s_time = time.time()
        self.graph.run(wf_query)
        e_time = time.time()
        print("Time elapsed in creating database: %d seconds" % int(e_time - s_time))

    def _build_load_io_data_from_csv(self, column_map, file_name):
        """
        Build query string for bulk import of Tools IO data.
        """

        if self.components['Nodes']['ToolInput'] in column_map:
            io_node = 'ToolInput'
        else:
            io_node = 'ToolOutput'

        tool = '{0}{{name:source.{1}}}'.format(
            self.components['Nodes']['Tool'],
            column_map[self.components['Nodes']['Tool']]
        )
        version = '{0}{{name:source.{1}}}'.format(
            self.components['Nodes']['Version'],
            column_map[self.components['Nodes']['Version']]
        )
        dataset = '{0}{{name:source.{1}}}'.format(
            self.components['Nodes'][io_node],
            column_map[self.components['Nodes'][io_node]]
        )
        datatype = '{0}{{name:source.{1}}}'.format(
            self.components['Nodes']['Datatype'],
            column_map[self.components['Nodes']['Datatype']]
        )
        edam_format = '{0}{{id:source.{1}}}'.format(
            self.components['Nodes']['EDAMFormat'],
            column_map[self.components['Nodes']['EDAMFormat']]
        )

        query = (
            "LOAD CSV "
            "WITH HEADERS FROM 'file:///{fn}' AS source "
            "WITH source "
            "MERGE (t:{tool}) MERGE (dt:{datatype}) MERGE (fmt:{edam_format}) "
            "WITH source, t, dt, fmt "
            "MERGE (dt)-[:{dtfmt_rel}]->(fmt) "
            "MERGE (t)<-[:{tv_rel}]-(v:{version}) "
            "WITH source, v, dt "
            "MERGE (v)-[:{vio_rel}]->(d:{dataset}) "
            "WITH source, d, dt "
            "MERGE (d)-[:{dt_rel}]->(dt) "
        ).format(
            fn=os.path.basename(file_name),
            tool=tool,
            version=version,
            dataset=dataset,
            datatype=datatype,
            edam_format=edam_format,
            tv_rel=self.components['Relationships']['Tool_to_Version'],
            vio_rel=self.components['Relationships'][
                'Version_to_{0}'.format(io_node)
            ],
            dt_rel=self.components['Relationships'][
                '{0}_to_Datatype'.format(io_node)
            ],
            dtfmt_rel=self.components['Relationships']['Datatype_to_EDAMFormat']
        )

        return query

    def load_io_data_from_csv(self, file_name, io_node_type):
        """Bulk import Tools IO data into db.

        Run with io_node_type="ToolInput" to upload tool input data,
        with io_node_type="ToolOutput" to upload tool output data.

        To make the import work, copy the csv file to the neo4j
        # /import folder and pass the full path to the file via
        file_name.
        """

        with open(file_name, 'r') as i:
            # associate db Nodes with csv column names
            column_map = dict(
                zip(
                    (
                        self.components['Nodes']['Tool'],
                        self.components['Nodes']['Version'],
                        self.components['Nodes'][io_node_type],
                        self.components['Nodes']['Datatype'],
                        self.components['Nodes']['EDAMFormat']
                    ),
                    i.readline().strip().split(',')
                )
            )

        # build the cypher query string
        query = self._build_load_io_data_from_csv(column_map, file_name)
        print(query)

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
        i_name = "trimmomatic"
        o_name = "bowtie2"
        get_all_nodes_query = "MATCH (n) RETURN n"
        all_nodes = self.graph.run(get_all_nodes_query).data()
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
        #query5 = "MATCH (a:Tool {name: {name_a}}) - [r*..6] -> (b:Tool) RETURN COLLECT(distinct b.name) as predicted_tools"
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

        graph_db.load_io_data_from_csv("/home/kumara/Graphdatabases/galaxy_neo_tools_graph/data/tool_iformats.csv", "ToolInput")
        graph_db.load_io_data_from_csv("/home/kumara/Graphdatabases/galaxy_neo_tools_graph/data/tool_oformats.csv", "ToolOutput")
        graph_db.create_graph_bulk_merge(workflow_file)
    # run queries against database
    graph_db.fetch_records()
