import socket

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #AF_NET = IPv4; SOCK_STREAM = TCP (SOCK_DGRAM = UDP)
client.connect(('localhost',9009))

#client.send(b"I am CLIENT\n")
#from_server = client.recv(4096)
client.close()

#print (from_server)