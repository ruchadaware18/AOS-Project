import socket

def main():
    ip = "127.0.0.1"
    port = int(input("Enter the port number of the node: "))
    
    while True:
        print("******************* MENU *******************")
        print("Press 1 to insert a key-value pair")
        print("Press 2 to search for a key")
        print("Press 3 to delete a key")
        print("Press 4 to exit")
        print("********************************************")
        
        choice = input("Enter your choice: ")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip, port))

        if choice == '1':
            key = input("Enter the key: ")
            val = input("Enter the value: ")
            message = f"insert|{key}:{val}"
            sock.send(message.encode('utf-8'))
            data = sock.recv(1024).decode('utf-8')
            print(data)

        elif choice == '2':
            key = input("Enter the key: ")
            message = f"search|{key}"
            sock.send(message.encode('utf-8'))
            data = sock.recv(1024).decode('utf-8')
            print(f"The value corresponding to the key {key} is: {data}")

        elif choice == '3':
            key = input("Enter the key: ")
            message = f"delete|{key}"
            sock.send(message.encode('utf-8'))
            data = sock.recv(1024).decode('utf-8')
            print(data)

        elif choice == '4':
            print("Closing the socket")
            sock.close()
            print("Exiting the client")
            exit()

        else:
            print("INVALID CHOICE")


if __name__ == '__main__':
    main()
