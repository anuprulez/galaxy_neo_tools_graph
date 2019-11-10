import requests
import os
import json


class ToolInfo:

    def __init__(self, path_all_tools):
        """ Init method. """
        self.path_all_tools = path_all_tools
        self.GALAXY_URL = "https://usegalaxy.eu/api/tools/"
        
    def read_tools(self):
        """
        Iterate over all tools and collect tool ids
        """
        all_tools = list()
        tool_ids = list()
        # read json file containing all tools
        with open(self.path_all_tools, 'r') as f_tools:
            all_tools = json.loads(f_tools.read())
        for item in all_tools:
            if 'model_class' in item:
                if "elems" in item:
                    tool_section = item["elems"]
                    for tool in tool_section:
                        if "model_class" in tool and tool["model_class"] == "Tool":
                            tool_ids.append(tool['id'])
        assert len(tool_ids) > 0
        return tool_ids


    def fetch_tool(self, tool_ids):
        """
        Download the model from remote server
        """
        for t_id in tool_ids:
            t_url = self.GALAXY_URL + t_id + "?io_details=True"
            tool_info = json.loads(requests.get(t_url).text)
            print(tool_info)
            print()
