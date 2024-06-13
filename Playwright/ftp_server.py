from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
import os

def main():
    # Specify the path to the xmlData folder
    xml_data_path = os.path.join(os.getcwd(), 'xmlData')
    
    # Create xmlData directory if it doesn't exist
    os.makedirs(xml_data_path, exist_ok=True)

    # Create a DummyAuthorizer for managing 'virtual' users
    authorizer = DummyAuthorizer()
    print(xml_data_path)
    # Define a user with name, password, and permissions
    # Use strong, secure credentials in a production environment!
    username = "user"
    password = "password123"
    authorizer.add_user(username, password, xml_data_path, perm='elradfmwMT')

    # Instantiate FTP handler class
    handler = FTPHandler
    handler.authorizer = authorizer

    # Define a customized banner (shown when client connects)
    handler.banner = "Welcome to the xmlData FTP server."

    # Specify a masquerade address and the range of ports to use for passive connections
    handler.masquerade_address = '127.0.0.1'
    handler.passive_ports = range(60000, 65535)

    # Instantiate FTP server class and listen on 0.0.0.0:2121
    address = ('', 2121)
    server = FTPServer(address, handler)

    # Set a limit for connections
    server.max_cons = 256
    server.max_cons_per_ip = 5

    # Start ftp server
    server.serve_forever()

if __name__ == '__main__':
    main()