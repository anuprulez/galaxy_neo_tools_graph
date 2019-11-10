import requests
import os
import json


class ToolInfo:

    def __init__(self, path_all_tools):
        """ Init method. """
        self.path_all_tools = path_all_tools
        
    def read_tools(self):
        """
        Iterate over all tools
        """
        all_tools = list()
        tool_ids = list()
        tool_sections = list()
        with open(self.path_all_tools, 'r') as f_tools:
            all_tools = json.loads(f_tools.read())
        for item in all_tools:
            if 'model_class' in item:
                tool_sections.append(item['model_class'])
                print()
                if "elems" in item:
                    tool_section = item["elems"]
                    for tool in tool_section:
                        print(tool)
                        print()
                print("-----------------------")
        print(len(tool_sections))


    def fetch_tool(self, url):
        """
        Download the model from remote server
        """
        local_dir = os.path.join(os.getcwd(), 'tools.json')
        # read model from remote
        model_binary = requests.get(model_url)
        # save model to a local directory
        with open(local_dir, 'wb') as model_file:
            model_file.write(model_binary.content)
            return local_dir
        print(fetch)
