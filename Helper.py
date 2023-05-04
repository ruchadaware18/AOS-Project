import socket

def main():
    # ip = input("Give the ip address of a node")
    ip = "127.0.0.1"
    # port = 9000
    port = int(input("Give the port number of a node: "))
    
    while True:
        print("************************MENU*************************")
        print("PRESS ***********************************************")
        print("1. TO ENTER *****************************************")
        print("2. TO SHOW ******************************************")
        print("3. TO DELTE *****************************************")
        print("4. TO EXIT ******************************************")
        print("*****************************************************")
        choice = input().strip()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((ip, port))
            if choice == '1':
                key = input("ENTER THE KEY: ").strip()
                val = input("ENTER THE VALUE: ").strip()
                message = f"insert|{key}:{val}"
                sock.sendall(message.encode('utf-8'))
                data = sock.recv(1024).decode('utf-8').strip()
                print(data)
            elif choice == '2':
                key = input("ENTER THE KEY: ").strip()
                message = f"search|{key}"
                sock.sendall(message.encode('utf-8'))
                data = sock.recv(1024).decode('utf-8').strip()
                print(f"The value corresponding to the key is : {data}")
            elif choice == '3':
                key = input("ENTER THE KEY: ").strip()
                message = f"delete|{key}"
                sock.sendall(message.encode('utf-8'))
                data = sock.recv(1024).decode('utf-8').strip()
                print(data)
            elif choice == '4':
                print("Closing the socket")
                print("Exiting Client")
                break
            else:
                print("INCORRECT CHOICE")

if __name__ == '__main__':
    main()
