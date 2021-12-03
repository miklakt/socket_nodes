import logging
from .libserver import Server, ServerNoDisconnectionAllowed
import threading
import subprocess

def create_server_and_nodes(scripts: list,
        args_list = None, python_executable = 'python',
        ServerClass = ServerNoDisconnectionAllowed,
        **popen_kwargs
        ):
    server = ServerClass()
    threading.Thread(target=server.run, daemon=True).start()
    print('Server started')
    if args_list is None:
        args_list = [[]]*len(scripts)
    for script, args in zip(scripts, args_list):
        popen_list = [
            python_executable, script,
            server.IP, server.PORT,
            *args
            ]
        popen_list = [str(item) for item in popen_list]
        print(f"Popen({popen_list}, {popen_kwargs})")
        subprocess.Popen(popen_list, **popen_kwargs)
        server.wait_connection(timeout = 600)
    return server
