import argparse
import os
import time


from py2neo import Graph


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
                'InTool': 'Tool',
                'OutTool': 'Tool',
                'InToolV': 'Version',
                'OutToolV': 'Version'
            },
            'Relationships': {
                'Tool_to_Version': 'HAS_VERSION',
                'Version_to_ToolOutput': 'GENERATES_OUTPUT',
                'Version_to_ToolInput': 'FEEDS_INTO',
                'WorkflowConnection_to_ToolOutput': 'IS_CONNECTED_BY',
                'WorkflowConnection_to_ToolInput': 'TO_INPUT',
                'Workflow': 'WORKFLOW',
                'ToolOutput_to_Datatype': 'HAS_DATATYPE',
                'ToolInput_to_Datatype': 'HAS_DATATYPE',
                'Datatype_to_EDAMFormat': 'IS_OF_FORMAT',
            }
        }

    def create_graph_bulk_merge(self, file_path):
        """
        Create graph database with bulk import
        """
        # To make this query work, copy the csv file to /var/lib/neo4j/import/ and just pass the file name for the argument 'wf'
        with open(file_path, 'r') as i:
            # associate db Nodes with csv column names
            wf_column_map = dict(
                zip(
                    (
                        'WfId',
                        'InTool',
                        'InToolV',
                        'ToolOutput',
                        'OutTool',
                        'ToolInput',
                        'OutToolV'
                    ),
                    i.readline().strip().split(',')
                )
            )

        in_tool = '{0}{{name:tc.{1}}}'.format(
            self.components['Nodes']['Tool'],
            wf_column_map['InTool']
        )

        out_tool = '{0}{{name:tc.{1}}}'.format(
            self.components['Nodes']['Tool'],
            wf_column_map['OutTool']
        )

        in_version = '{0}{{name:tc.{1}}}'.format(
            self.components['Nodes']['Version'],
            wf_column_map['InToolV']
        )

        out_version = '{0}{{name:tc.{1}}}'.format(
            self.components['Nodes']['Version'],
            wf_column_map['OutToolV']
        )

        output_dataset = '{0}{{name:tc.{1}}}'.format(
            self.components['Nodes']['ToolOutput'],
            wf_column_map['ToolOutput']
        )

        input_dataset = '{0}{{name:tc.{1}}}'.format(
            self.components['Nodes']['ToolInput'],
            wf_column_map['ToolInput']
        )

        workflow = '{0}{{name:tc.{1}}}'.format(
            self.components["Nodes"]["Workflow"],
            wf_column_map['WfId']
        )

        wf_query = (
            "LOAD CSV WITH HEADERS FROM 'file:///{file_name}' AS tc "
            "MERGE (in_tool:{in_tool}) MERGE (out_tool: {out_tool}) MERGE (wf:{workflow}) "
            "WITH tc, in_tool, out_tool, wf "
            "MERGE (in_tool)-[:{tv_rel}]->(in_v:{in_version}) "
            "MERGE (out_tool)-[:{tv_rel}]->(out_v:{out_version}) "
            "WITH tc, in_v, out_v, wf "
            "MERGE (in_v)-[:{v_out}]->(d_out:{output_dataset}) "
            "MERGE (out_v)<-[:{v_in}]-(d_in: {input_dataset}) "
            "WITH tc, d_out, d_in, wf "
            "MERGE (d_out)-[:{conn_out}]->(wf_conn:{wf_conn}{{}})-[:{conn_in}]->(d_in) "
            "MERGE (wf_conn) -[:{workflow_rel}] ->(wf)"
        ).format(
            file_name=os.path.basename(file_path),
            in_tool=in_tool,
            tv_rel=self.components['Relationships']['Tool_to_Version'],
            in_version=in_version,
            v_out=self.components['Relationships']['Version_to_ToolOutput'],
            output_dataset=output_dataset,
            out_tool=out_tool,
            out_version=out_version,
            v_in=self.components["Relationships"]['Version_to_ToolInput'],
            input_dataset=input_dataset,
            conn_out=self.components["Relationships"]["WorkflowConnection_to_ToolOutput"],
            wf_conn=self.components["Nodes"]["WorkflowConnection"],
            conn_in=self.components["Relationships"]["WorkflowConnection_to_ToolInput"],
            workflow_rel=self.components["Relationships"]["Workflow"],
            workflow=workflow
        )

        print("Creating database in bulk...")
        print(wf_query)
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
            io_rel = '<-[:{0}]-'.format(self.components['Relationships']['Version_to_ToolInput'])
        else:
            io_node = 'ToolOutput'
            io_rel = '-[:{0}]->'.format(self.components['Relationships']['Version_to_ToolOutput'])

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
            "MERGE (t)-[:{tv_rel}]->(v:{version}) "
            "WITH source, v, dt "
            "MERGE (v){io_rel}(d:{dataset}) "
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
            io_rel=io_rel,
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

    def create_index(self, node):
        self.graph.schema.create_index(node, "name")

    def fetch_records(self):
        print("Fetching records...")
        print()
        s_time = time.time()
        get_all_nodes_query = "MATCH (n) RETURN n"
        all_nodes = self.graph.run(get_all_nodes_query).data()
        print("Number of all nodes: %d" % len(all_nodes))
        print()
        e_time = time.time()
        print("Time elapsed in fetching records: %d seconds" % int(e_time - s_time))


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-url", "--url", required=True, help="Neo4j server")
    arg_parser.add_argument("-un", "--user_name", required=True, help="User name")
    arg_parser.add_argument("-pass", "--password", required=True, help="Password")
    arg_parser.add_argument("-cd", "--create_database", required=True, help="Create a new database or not")
    arg_parser.add_argument("-ti", "--tool_inputs_file", required=True, help="Tool inputs file")
    arg_parser.add_argument("-to", "--tool_outputs_file", required=True, help="Tool outputs file")
    arg_parser.add_argument("-wf", "--workflow_file", required=True, help="Workflow file")
    args = vars(arg_parser.parse_args())
    url = args["url"]
    username = args["user_name"]
    password = args["password"]
    create_db = args["create_database"]
    t_inputs_file = args["tool_inputs_file"]
    t_output_file = args["tool_outputs_file"]
    workflow_file = args["workflow_file"]
    # connect to neo4j database
    graph_db = WorkflowGraphDatabase(url, username, password)
    # create a database after deleting the existing records
    if create_db == "true":
        n = graph_db.graph.delete_all()
        assert n == None
    graph_db.create_graph_bulk_merge(workflow_file)
    graph_db.load_io_data_from_csv(t_output_file, "ToolOutput")
    graph_db.load_io_data_from_csv(t_inputs_file, "ToolInput")
    graph_db.create_index(graph_db.components["Nodes"]["Tool"])
    graph_db.create_index(graph_db.components["Nodes"]["Datatype"])
    # run queries against database
    graph_db.fetch_records()
