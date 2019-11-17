import json
import os
from urllib.parse import urljoin

import requests


GALAXY_BASE_URL = 'https://usegalaxy.eu'


class APITools():
    """Base class for interacting with /api/tools of a Galaxy instance."""

    api_tools_path = 'api/tools/'

    def __init__(self, data):
        raise NotImplementedError(
            'Cannot initialize instance of APITools baseclass.'
        )

    @classmethod
    def _from_query(cls, url):
        this = cls(requests.get(url).json())
        this.from_url = url
        return this

    @classmethod
    def _get_base_url(cls, galaxy_base=None):
        if galaxy_base is None:
            galaxy_base = GALAXY_BASE_URL
        return urljoin(galaxy_base, cls.api_tools_path)

    def __getitem__(self, item):
        return self.data[item]


class APIToolbox(APITools):
    """Parse the toolbox of a Galaxy instance."""
    
    def __init__(self, toolbox_json):
        """Initialize a toolbox from a server response stored as JSON."""
        
        self.data = toolbox_json
        self.from_url = None

    @classmethod
    def from_query(cls, galaxy_base=None):
        """Initialize the toolbox by performing a server toolbox query."""
        
        url = cls._get_base_url(galaxy_base)
        return cls._from_query(url)

    def get_tools(self):
        """Iterator over the JSON-formatted tools defined in the toolbox."""
        for item in self.data:
            item_type = item.get('model_class')
            if item_type == 'Tool':
                yield item
            elif item_type == 'ToolSection':
                for inner_item in item['elems']:
                    if inner_item.get('model_class') == 'Tool':
                        yield inner_item

    def __len__(self):
        """Return the length of the toolbox.

        Caveat: This is generally NOT corresponding to the number of
        tools in the toolbox (because tools can be / are usually stored
        inside sections.

        Use len(list(toolbox.get_tools())) to get the number of tools in the
        toolbox.
        """

        return len(self.data)


class APIToolIO(APITools):
    """Parse the IO details of a specific tool from a Galaxy instance."""

    io_details_path = '?io_details=True'

    def __init__(self, toolio_json):
        """Initialize the IO details from a server response stored as JSON."""

        self.data = toolio_json
        self.from_url = None

    @classmethod
    def from_query(cls, tool_id, galaxy_base=None):
        """Initialize the IO details by performing a fresh server query."""
        url = urljoin(
            cls._get_base_url(galaxy_base),
            tool_id + cls.io_details_path
        )
        return cls._from_query(url)

    def get_tool_inputs(self):
        """Get the input data information for the represented tool."""

        ret = []
        for i in self._parse_input_section(self.data['inputs']):
            if i not in ret:
                ret.append(i)
        return ret

    def _parse_input_section(self, input_section, prefix=None):
        if prefix:
            prefix = prefix + '|'
        else:
            prefix = ''
        for item in input_section:
            item_type = item.get('model_class')
            if item_type == 'DataToolParameter':
                yield prefix + item['name'], item['extensions'], item['edam']                
            elif item_type == 'Conditional':
                for case in item['cases']:
                    assert case['model_class'] == 'ConditionalWhen'
                    yield from self._parse_input_section(
                        case['inputs'], prefix=prefix+item['name']
                    )
            elif item_type == 'Repeat':
                yield from self._parse_input_section(
                    item['inputs'], prefix=prefix+item['name']
                )

    def get_tool_outputs(self):
        """Get the output data information for the represented tool."""

        ret = []
        for item in self.data['outputs']:
            item_type = item.get('model_class')
            assert item_type in ['ToolOutput', 'ToolOutputCollection']
            # to do: handle collections
            if item_type == 'ToolOutput':
                ret.append((
                    item['name'],
                    [item['format']],
                    {
                        'edam_data': [item['edam_data']] if 'edam_data' in item else [],
                        'edam_formats': [item['edam_format']] if 'edam_format' in item else []
                    }
                ))
        return ret
