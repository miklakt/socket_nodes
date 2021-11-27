import socket
import logging
import pickle
import select
from enum import Enum, auto
from typing import List
import sys
import time

logger = logging.getLogger(__name__)

class RequestStatus(Enum):
    """
    Possible request status
    """
    InProgress = auto()
    Done = auto()
    Disconnected = auto()
    #ErrorOccurred = auto()
    #Timeout = auto()



class NoResult:
    def __repr__(self) -> str:
        return "NoResult"



class RequestClass:

    """
    Request class gives access to a result of an request from Server class
    holds NoResult at initialization until acquire the result of the request
    from the node

    Use self.result() to wait for result, if not threaded can block the program
    """

    def __init__(self, request) -> None:
        """Initializes request

        Args:
            request (pickleable object): request data
        """
        self.request = request
        self.status = RequestStatus.InProgress
        self._result = NoResult

    def __bool__(self):
        """Check if request is done

        Returns:
            [bool]: True if request status is done
        """
        return self.status == RequestStatus.Done

    def result(self):
        """Waits for the result of the request and returns it

        Returns:
            [pickleable object]: request's result
        """
        while True:
            if self.status == RequestStatus.Done:
                return self._result
            if self.status == RequestStatus.Disconnected:
                return self._result

    def __repr__(self) -> str:
        return f"Status: {self.status}\n>>> {self.request}\n<<< {self._result}"



class ConnectedNode:
    """Contains information about a connected node, keeps track of the
    assigned requests from the server, provides RequestClass object as
    a placeholder for future result

    requests : List[RequestClass] - list of queued requests FIFO

    """
    ###! ids is not the same as node_idx in Server class
    # this will increment every class instantiation
    # and will not change on node disconnection
    # while in the Server instance node_idx is just an index in the
    # List[ConnectedNode]
    # it is not critical since used for logging only
    # TO DO: make indexing consistent
    ids = int(0)
    def __init__(self, socket) -> None:
        self.socket = socket
        self.requests : List[RequestClass] = []
        self.ids = ConnectedNode.ids
        ConnectedNode.ids+=1

    def is_busy(self):
        return bool(self.requests)

    def add_request(self, request_data):
        """adds request to a queue, so that server side knows about
        assigned request, returns RequestClass object
        as a placeholder for future result,
        put the request to the end of FIFO self.requests : List[RequestClass]

        Returns:
            [RequestClass]: Request object
        """
        Request = RequestClass(request_data)
        self.requests.append(Request)
        return Request

    def finish_request(self, result):
        """Set result to the fist request object in the queue and pop it,
        now the result can be accessed with Request.result()

        """
        Request = self.requests.pop(0)
        Request._result = result
        Request.status = RequestStatus.Done
        #if debug, we are already getting all the info needed
        if not logger.isEnabledFor(logging.DEBUG):
            logger.info(f'node_{self.ids} : {Request.request} -> {result}')

    def __del__(self):
        logger.warning('Node is disconnected')
        while self.requests:
            Request = self.requests.pop(0)
            Request.status = RequestStatus.Disconnected
            logger.error(f'{self.ids} : {Request.request} can not be executed, the node is disconnected')
            Request._result = NoResult



class Server():
    """
    Multiclient TCP server, where the
    """
    IP : str
    PORT : int
    active : bool
    nodes : List[ConnectedNode] ##should be refactored into collection
    stop_if_no_one_connected : bool = True #if last node disconnected, stop the server

    def __init__(self, IP : str = '127.0.0.1', PORT : int = 0, setup : bool =True) -> None:
        """Initialize an object, set PORT to zero to allow OS assign it

        Args:
            IP ([str]): host ip
            PORT ([int]): port to open
        """
        self.IP = IP
        self.PORT = PORT
        self.active = False
        self.nodes = []
        logger.info(f'Initialized with {IP}, {PORT}')
        if setup:
            self.setup()


    def setup(self):
        """setup the TCP socket server that gather info from the nodes,
        make server available for nodes to connect
        """
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.setblocking(0)
        server_socket.bind((self.IP, self.PORT))
        server_socket.listen(0)
        self.active = True
        self.socket = server_socket
        if self.PORT==0:
            #the port has been assigned by OS
            self.PORT=self.socket.getsockname()[1]
            logger.info(f'The port has been assigned by OS, PORT : {self.PORT}')
        logger.info(f'Started')


    def wait_for_connections(self, n : int, timeout : int = 60) -> None:
        """Wait for n nodes to connect, should be used to assure that all
        clients are connected
        Args:
            n (int): number of nodes to wait
        """
        logger.info(f"Server is expecting to connect {n} nodes")
        start_time = time.time()
        while len(self.nodes)<n:
            if time.time() - start_time > timeout:
                logger.error("Waiting time exceeds timeout")
                raise TimeoutError("Waiting time exceeds timeout")
            else:
                pass

    def wait_connection(self, timeout : int = 60) -> None:
        """Wait for new connection
        Args:
            timeout (int, optional): Timeout in seconds. Defaults to 10.
        """
        logger.info(f"Server is expecting to connect one more node")
        n_nodes = len(self.nodes)
        start_time = time.time()
        while len(self.nodes) == n_nodes:
            if time.time() - start_time > timeout:
                logger.error("Waiting time exceeds timeout")
                raise TimeoutError("Waiting time exceeds timeout")
            else:
                pass


    def handle_connection(self):
        """
        Handles connection of a new node,
        creates an instance of ConnectedNode class then append it to
        to a list of connected nodes self.nodes : List[ConnectedNode]
        """
        node_socket, node_address = self.socket.accept()
        Node = ConnectedNode(node_socket)
        self.nodes.append(Node)

        logger.info(f'Accepted new connection from {node_address}')
        logger.debug(f'Nodes connected: {len(self.nodes)}')


    def handle_disconnection(self, node_id : int):
        """Disconnection of the node, deletes it from the list of connected
        nodes self.nodes : List[ConnectedNode]

        TODO : there could be unfinished request to deal with

        Args:
            node_id (int): node index in the list of connected nodes (self.nodes)
        """
        logger.warning(f'Connection with {node_id} has been closed')
        del self.nodes[node_id]
        if len(self.nodes) == 0:
            logger.warning("No nodes connected")
            if self.stop_if_no_one_connected:
                self.shutdown()


    def handle_income(self, income_data, node_id : int):
        """Handle an income, by finnishing FIFO request of the node

        Args:
            income_data ([type]): responce from the node
            node_id (int): node index in the list of connected nodes (self.nodes)
        """

        #logger.debug(f'income from {self.nodes[node_id].socket.getpeername()}')
        self.nodes[node_id].finish_request(income_data)


    def listen(self):
        """
        TCP server listens to several nodes (TCP Clients) and new connections
        invokes handlers depending on the event
        """
        #list of the sockets including connected nodes sockets and the server one
        sockets_list = [node.socket for node in self.nodes]+[self.socket]
        #find the socket where reading is happening
        read_sockets, _, exception_sockets = select.select(sockets_list, [], sockets_list)
        for notified_socket in read_sockets:

            #if server socket -> new connection
            if notified_socket == self.socket:
                self.handle_connection()

            #if node socket notified -> new income from the node
            else:
                #find the id of the Node sending the data

                node_id = self._get_node_idx_by_socket(notified_socket)#cumbersome
                #recv data from the node socket
                income_data = self.recv_raw(notified_socket)
                #logger.debug(f'Notified socket: {notified_socket.getpeername()}, node_id: {node_id}, income data: {income_data}')
                #if no data -> client disconnected
                if income_data is False:
                    self.handle_disconnection(node_id)
                    continue
                #handle income data, usually by finnishing some request
                self.handle_income(income_data, node_id)

        #error
        for notified_socket in exception_sockets:
            # client disconnected violently
            self.handle_disconnection(notified_socket)

        #timeout
        if not (read_sockets, _, exception_sockets):
            #not yet implemented
            logger.warning('listen loop timeout')
            pass


    def request_to_one_node(self, request_data, node_id : int) -> RequestClass:
        """Makes a request to a node with index node_id, returns an instance of
         the Request class, which allows to collect the result later

        Args:
            request_data (object): request body
            node_id (int): node index in the list of connected nodes (self.nodes)

        Returns:
           RequestClass: Request object, get the result with _.result()
        """
        #logger.debug(f'Creating new request to {node_id}, {self.nodes[node_id].socket}')
        #adds request to the queue of the corresponding node
        Request =  self.nodes[node_id].add_request(request_data)
        #gets the node id
        node_socket = self.nodes[node_id].socket
        #send the data to the node
        self.send_raw(node_socket, request_data)
        return Request


    def request_to_multiple_nodes(self, request_data, node_indices : List[int]) -> List[RequestClass]:
        """Broadcast request to multiple nodes

        Args:
            request_data ([type]): request body
            node_indices (List[int]): node indices in the list of connected nodes (self.nodes)

        Returns:
            List[RequestClass]: list of RequestClass object, get the result with _.result()
        """
        return [self.request_to_one_node(request_data, node_id) for node_id in node_indices]


    def request(self, request_data, node_id):
        if isinstance(node_id, int):
            return self.request_to_one_node(request_data, node_id)
        elif isinstance(node_id, list):
            return self.request_to_multiple_nodes(request_data, node_id)


    def wait_node(self, node_id : int):
        """Waits for the node to finnish all the requests, call if you want
        to make sure that everything is done with this node
        Args:
            node_id (int): node index in the list of connected nodes (self.nodes)
        """
        logger.info(f'Waiting node {node_id} to finnish...')
        while self.nodes[node_id].is_busy()==False:
            pass
        logger.info(f'Node {node_id} has finished all requests')


    def wait_all_nodes(self):
        """Waits for the all nodes to finnish all the requests, call if you want
        to make sure that everything is done with the all connected nodes

        Args:
            node_id (int): node index in the list of connected nodes (self.nodes)
        """
        logger.info(f'Waiting all nodes to finnish...')
        while any([node.is_busy() for node in self.nodes]):
            pass
        logger.info(f'All nodes have finished all requests')

    def send_raw(self, node_socket, data):
        """Sending protocol is implemented here, allows to send any
        pickable python object, can be overridden

        Args:
            node_socket (socket): node's socket
            data (object): data to send
        """
        try:
            HEADER_LENGTH = 10
            msg = pickle.dumps(data)
            msg = bytes(f"{len(msg):<{HEADER_LENGTH}}", 'utf-8')+msg
            node_socket.send(msg)
            if logger.isEnabledFor(logging.DEBUG):
                node_idx = str(self._get_node_idx_by_socket(node_socket))
                trim_length = 100
                str_data = str(data)
                str_data = (str_data[:trim_length] + '...') if len(str_data) > trim_length else str_data
                logger.debug('>>> '+node_idx+" : " + str_data)
        except Exception as e:
            logger.exception(e)
            raise e


    def recv_raw(self, node_socket):
        """Receiving protocol is implemented here, allows to send any
        pickable python object, can be overridden

        Args:
            node_socket (socket): node's socket
        """
        HEADER_LENGTH = 10
        try:
            message_header = node_socket.recv(HEADER_LENGTH)
            #no data -> client closed a connection
            if not len(message_header):
                return False
            message_length = int(message_header.decode('utf-8').strip())
            serialized_data = node_socket.recv(message_length)
            data = pickle.loads(serialized_data)
            if logger.isEnabledFor(logging.DEBUG):
                node_idx = str(self._get_node_idx_by_socket(node_socket))
                trim_length = 100
                str_data = str(data)
                str_data = (str_data[:trim_length] + '...') if len(str_data) > trim_length else str_data
                logger.debug('<<< '+node_idx+" : " + str_data)
            return data
        except Exception as e:
            logger.exception(e)
            return False


    def run(self):
        """The main loop of the server, listens for new connection or income
        data, while the server is active.
        Blocking call, use threading or multiproccessing
        """
        logger.info("server main loop has started")
        while self.active:
            self.listen()
        logger.info("server main loop done")


    def shutdown(self):
        """Shuts the server down, no extra actions are yet implemented
        """
        self.active = False
        self.nodes = []
        self.socket.close()
        logger.info("The server is shutted down")


    def _get_node_idx_by_socket(self, socket_):
        """Returns node index (id) in the self.nodes : List[ConnectedNode]
        for a given socket object

        Args:
            address (str): [description]

        Returns:
            [type]: [description]
        """
        sockets_ = [Node.socket for Node in self.nodes]
        idx = sockets_.index(socket_)
        return idx


    def __call__(self, *args):
        return self.request(*args)


class ServerNoDisconnectionAllowed(Server):
    def handle_disconnection(self, node_id: int):
        super().handle_disconnection(node_id)
        logging.error("Node is disconnected, not expected for libserver.ServerNoDisconnectionAllowed")
        self.shutdown()