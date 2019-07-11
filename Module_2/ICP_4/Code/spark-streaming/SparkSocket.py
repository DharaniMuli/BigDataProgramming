import socket
import sys
if __name__ == '__main__':

    file = open("D:\\Datasets\\Text\\20 NG\\20ng-train-all-terms.txt", "r")
    filesss = file.readlines()

    HOST = ''
    PORT = 1234

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print('# Socket created')

    # Create socket on port
    try:
        s.bind((HOST, PORT))
    except socket.error as msg:
        print('# Bind failed. ')
        sys.exit()

    print('# Socket bind complete')

    # Start listening on socket
    s.listen(10)
    print('# Socket now listening')

    # Wait for client
    conn, addr = s.accept()
    print('# Connected to ' + addr[0] + ':' + str(addr[1]))

    # Receive data from client
    while True:
        #data = conn.recv(1024)
        #line = data.decode('UTF-8')    # convert to string (Python 3 only)
        #line = line.replace("\n","")   # remove newline character
        for filestr in filesss:
            conn.send(str(filestr).encode('UTF-8'))

    s.close()