import logging
import pickle
import socket

logger = logging.getLogger(__name__)

class BaseNode:
    """Class implementing a node connected to a server,
    capable of to handle request. Can be used for distributed computing.
    Connected node listens to the socket for requests and sends back
    the results when one is available
    """
    connected : bool = False
    IP : str
    PORT : int

    def __init__(self, IP : str, PORT : int) -> None:
        """Initialize the node, without connection
        Args:
            IP (str), PORT (int): Must be the same as on the server side
        """
        self.IP =IP
        self.PORT = PORT
        logger.info(f'Initialized with {self.IP}, {self.PORT}')

    def connect(self) -> bool:
        """Connects to the server

        Returns:
            [bool]: True if success
        """
        logger.debug('Connecting...')
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((self.IP, self.PORT))
        self.connected = True
        logger.info('Connected to the server')
        return True

    def run(self):
        """The main loop of the node
        0)Connects to the server
        1)Listen for incoming data
        2)Process request
        3)Send the results back
        """
        self.connect()

        #while the note is still connected
        while self.connected:
            #logger.debug('\nListening...')
            #get the data from the socket
            data = self.recv_raw()
            #if no data -> connection is lost
            if data == False:
                self.handle_disconnection()
                break
            else:
                self.handle_request(data)
        return True

    def execute(self, request):
        """Trying to execute the request, the method has to be overridden
        for child classes by default tries to eval(request)

        Args:
            request (object): valid request

        Returns:
            object: execution result
        """
        logger.debug('Trying to execute with eval()')
        result = eval(request)
        logger.debug(f'Result: {result}')
        return result

    def verify(self, request) -> bool:
        """Verify if request is correct,
        the method has to be overridden if some checks needed

        Args:
            request (object): request

        Returns:
            bool: True if request is valid
        """
        return True

    def handle_request(self, request):
        """Handles request from server, send the result back when available
        Args:
            request (object): [description]
        """
        #if request pass sanity check
        if self.verify(request):
            result = self.execute(request)
        #if not send the error back
        else:
            logger.error('Invalid request')
            result = 'Invalid request'
        try:
            self.send_raw(result)
        #can not be send, usually non-pickable
        except Exception as e:
            logger.exception(e)
            self.send_raw(e)

    def recv_raw(self):
        """Receiving protocol is implemented here, allows to send any
        pickable python object, can be overridden

        Args:
            node_socket (socket): node's socket
        """
        HEADER_LENGTH = 10
        try:
            message_header = self.server_socket.recv(HEADER_LENGTH)
            #no data -> client closed a connection
            if not len(message_header):
                return False
            message_length = int(message_header.decode('utf-8').strip())
            serialized_data = self.server_socket.recv(message_length)
            data = pickle.loads(serialized_data)
            if logger.isEnabledFor(logging.DEBUG):
                trim_length = 100
                str_data = str(data)
                str_data = (str_data[:trim_length] + '...') if len(str_data) > trim_length else str_data
                logger.debug('<<< '+str_data)
            return data
        except Exception as e:
            logger.exception(e)
            return False

    def send_raw(self, data):
        """Sending protocol is implemented here, allows to send any
        pickable python object, can be overridden

        Args:
            node_socket (socket): node's socket
            data (object): data to send
        """
        HEADER_LENGTH = 10
        msg = pickle.dumps(data)
        msg = bytes(f"{len(msg):<{HEADER_LENGTH}}", 'utf-8')+msg
        self.server_socket.send(msg)
        if logger.isEnabledFor(logging.DEBUG):
            trim_length = 100
            str_data = str(data)
            str_data = (str_data[:trim_length] + '...') if len(str_data) > trim_length else str_data
            logger.debug('>>> '+str_data)
        return True

    def handle_disconnection(self):
        """
        Can be overridden to implement extra actions on disconnection
        """
        logger.warning('Disconnected from server')
        self.connected = False
        self.server_socket.close()
        logger.warning('Socket is closed')




class ExecutorNode(BaseNode):
    """Inherited from BaseNode, allows to define executor,
    by instantiating ExecutorClass
    requirements to ExecutorClass:
    should provide three methods:
        __init__()
        execute(request) -> result
        verify(request) -> bool

    it is advised to inherited from BaseExecutorClass from libexecutor.py

    """
    def __init__(self, IP : str, PORT : int, ExecutorClass, *args, **kwargs) -> None:
        """Initialize the node, without connection, instantiates ExecutorClass

        Args:
            IP (str): [description]
            PORT (int): [description]
            ExecutorClass ([type]): [description]
        """
        super().__init__(IP, PORT)
        logger.info(f'Instantiate ExecutorClass...')
        self.Executor = ExecutorClass(*args, **kwargs)
        logger.info(f'Executor created, requests will be delegated')

    def execute(self, request):
        return self.Executor.execute(request)

    def verify(self, request):
        return self.Executor.verify(request)
