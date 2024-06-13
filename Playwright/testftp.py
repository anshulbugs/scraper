import socket

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('', 2121))
server.listen(1)
print("Listening on port 2121")
conn, addr = server.accept()
print(f"Connected by {addr}")
conn.close()