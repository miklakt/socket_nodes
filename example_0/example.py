#%%
import threading
import subprocess
import socket_nodes.utils
import socket_nodes

import logging
logging.basicConfig(filename='log', level = logging.DEBUG)
socket_nodes.set_params(LOG_REQUESTS_INFO = False, ON_NODE_ERROR = 'raise_error')
#%%
server = socket_nodes.utils.create_server_and_nodes(
    scripts = ['example_node.py', 'example_node.py'],
    args_list=[['-log_name', 'node_0_log'],['-log_name', 'node_1_log']],
    python_executable = 'python')

# %%
request = []
for i in range(10):
    request.append(server(f'{i}', 0))
    request.append(server(f'{i}**2', 1))
    #request.append(server(f'{i}**2', 2))
# %%
print ([request_.result() for request_ in request])
# %%
server.shutdown()
# %%
