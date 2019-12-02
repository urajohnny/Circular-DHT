import threading, time, sys, socket

host = '127.0.0.1'
base_port = 50000
is_quit = False

peer_id = int(sys.argv[1])
peer_port = peer_id + base_port

peer_successor_1 = int(sys.argv[2])
peer_successor_1_port = peer_successor_1 + base_port

peer_successor_2 = int(sys.argv[3])
peer_successor_2_port = peer_successor_2 + base_port


# print("Listening...")


class UDPClient(threading.Thread):  # threading.Thread
    def __init__(self,):
        threading.Thread.__init__(self)
        self._running = True

    def terminate(self):
        self._running = False

    def run(self):
        global peer_successor_1, peer_successor_2, peer_successor_1_port, peer_successor_2_port
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client.settimeout(1)
        seq = 0
        resp_seq = {}
        while True:
            if not self._running:
                break
            data = '{} {}'.format(seq, peer_id)
            client.sendto(data.encode(), (host, peer_successor_1_port))
            client.sendto(data.encode(), (host, peer_successor_2_port))
            seq += 1
            time.sleep(10)
            for _ in range(2):
                try:
                    data, addr = client.recvfrom(1024)
                    data = data.decode().split()
                    resp_seq[int(data[1])] = int(data[0])
                    # print(type(data[0]))
                    print('A ping response message was received from Peer {}'.format(int(data[1])))
                    if len(resp_seq) >= 3:
                        resp_seq = {}
                        seq = 0
                except:
                    pass
            for e in resp_seq:
                if seq - resp_seq[e] >= 5:
                    print('Peer {} is no longer alive.'.format(e))
                    data = '{}'.format(e)
                    if e == peer_successor_1:
                        time.sleep(5)
                        response = TCPClient_request(host, peer_successor_2_port, data)
                        response = response.decode().split()
                        peer_successor_1 = peer_successor_2
                        peer_successor_2 = int(response[0])

                    elif e == peer_successor_2:
                        response = TCPClient_request(host, peer_successor_1_port, data)
                        response = response.decode().split()
                        peer_successor_2 = int(response[1])
                        # TCPClient_response(host, peer_successor_1_port)
                    peer_successor_1_port = peer_successor_1 + base_port
                    peer_successor_2_port = peer_successor_2 + base_port
                    seq = 0  #initialize the seq
                    resp_seq = {}
                    print('My first successor is now peer {}.'.format(peer_successor_1))
                    print('My second successor is now peer {}.'.format(peer_successor_2))
                    break
                    # TCPClient_request(host, peer_successor_1_port, data)


class UDPServer(threading.Thread):  # threading.Thread
    def __init__(self, ):
        threading.Thread.__init__(self)
        self._running = True
        # self.server = server
        # self.host = host

    def terminate(self):
        self._running = False

    def run(self):
        predecessor = []
        global predecessor_1, predecessor_2

        server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server.bind((host, peer_port))
        while True:
            if not self._running:
                break
            try:
                data, addr = server.recvfrom(1024)
                data = data.decode().split()
                # if data[0] == 0:  #this is a request message
                print('A ping request message was received from Peer {}'.format(int(data[1])))
                # time.sleep(5)
                if int(data[1]) not in predecessor:
                    predecessor.append(int(data[1]))
                    if len(predecessor) >= 3:
                        predecessor = [int(data[1])]
                    elif len(predecessor) == 2:
                        predecessor_1 = predecessor[0]
                        predecessor_2 = predecessor[1]
                        # print(predecessor_1, predecessor_2)
                    else:
                        pass
                data = '{} {}'.format(int(data[0]), peer_id)
                server.sendto(data.encode(), addr)
            except:
                continue


def TCPClient_request(host, port, data):
    tcpsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpsocket.connect((host, port))
    tcpsocket.send(data.encode())
    response = tcpsocket.recv(1024)
    tcpsocket.close()
    if response:
        return response

class TCPClient(threading.Thread):
    def __init__(self, ):
        threading.Thread.__init__(self)
        self._running = True
        # self.peer_successor_1_port = peer_successor_1_port

    def terminate(self):
        self._running = False

    def run(self):
        global is_quit
        while True:
            if not self._running:
                break
            try:
                request = raw_input('').split()
                # request = request.split()
                # print('request', request)
                if request == '':
                    continue
                if request[0].lower() == 'request':
                    if len(request) > 2 or len(request) <= 1 or int(request[1]) < 0 or int(request[1]) > 9999:
                        raise ValueError
                    filename = int(request[1])
                    if filename == peer_id:
                        print('File {} is here.'.format(filename))
                    else:
                        data = '{} {} {}'.format(filename, peer_id, peer_id)
                        print('File request message for {} has been sent to my successor.'.format(filename))
                        TCPClient_request(host, peer_successor_1_port, data)
                elif request[0].lower() == 'quit':
                    data = '{} {} {} {}'.format(0, peer_id, peer_successor_1, peer_successor_2)
                    is_quit = True
                    # print('global flag changed')
                    TCPClient_request(host, predecessor_1 + 50000, data)
                    TCPClient_request(host, predecessor_2 + 50000, data)

            except ValueError:
                print('Wrong input.')


class TCPServer(threading.Thread):
    def __init__(self,):
        threading.Thread.__init__(self)
        self._running = True
        # self.peer_successor_1_port = peer_successor_1_port

    def terminate(self):
        self._running = False

    def run(self):
        global peer_successor_1, peer_successor_2, peer_successor_1_port, peer_successor_2_port
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((host, peer_port))
        server.listen(1)
        while True:
            if not self._running:
                break
            connectionSocket, addr = server.accept()
            data = connectionSocket.recv(1024)
            data = data.decode().split()
            if data[0] == 'file':
                print('Received a response message from peer {}, which has the file {}.'.format(data[2], data[1]))

            elif int(data[0]) == 0: # a peer quit gracefully
                print('Peer {} will depart from the network.'.format(int(data[1])))
                if peer_successor_1 == int(data[1]):
                    peer_successor_1 = int(data[2])
                    peer_successor_2 = int(data[3])
                elif peer_successor_2 == int(data[1]):
                    peer_successor_2 = int(data[2])

                peer_successor_1_port = peer_successor_1 + base_port
                peer_successor_2_port = peer_successor_2 + base_port
                print('My first successor is now peer {}.'.format(peer_successor_1))
                print('My second successor is now peer {}.'.format(peer_successor_2))

            elif len(data) == 1: # a peer died without notifying others
                data = '{} {}'.format(peer_successor_1, peer_successor_2)
                connectionSocket.send(data.encode())

            else:  # request file, data[0]=filename, data[1]=predecessor, data[2]= original peer
                if int(data[0]) % 256 <= peer_id and int(data[0]) % 256 > int(data[1]) \
                        or int(data[0]) % 256 > int(data[1]) and peer_id < int(data[1]) \
                        or int(data[0]) % 256 <= peer_id and int(data[1]) > peer_id:
                    print('File {} is here.'.format(int(data[0])))
                    print('A response message, destined for peer {}, has been sent.'.format(int(data[2])))
                    TCPClient_request(host, int(data[2])+50000, 'file {} {}'.format(int(data[0]), peer_id))
                else:
                    print('File {} is not stored here.'.format(int(data[0])))
                    TCPClient_request(host, peer_successor_1_port, '{} {} {}'.format(int(data[0]), peer_id, int(data[2])))
                    # connectionSocket.send(bytes([data[0], peer_id]))
                    print('File request message has been forwarded to my successor.')
            connectionSocket.close()


try:
    udpclient_threading = UDPClient()
    udpserver_threading = UDPServer()

    tcpclient_threading = TCPClient()
    tcpserver_threading = TCPServer()

    udpclient_threading.start()
    udpserver_threading.start()
    tcpclient_threading.start()
    tcpserver_threading.start()
    # print('all threadings start')
    while True:
        if is_quit:
            # print('is_quit')
            udpclient_threading.terminate()
            udpserver_threading.terminate()
            tcpclient_threading.terminate()
            tcpserver_threading.terminate()
            break

except:
    pass
