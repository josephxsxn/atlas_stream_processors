from scapy.all import *
from pymongo import MongoClient
from datetime import datetime
import argparse

# Set up command-line arguments
parser = argparse.ArgumentParser(description='Capture and store network packets in MongoDB')
parser.add_argument('--username', required=True, help='MongoDB username')
parser.add_argument('--password', required=True, help='MongoDB password')
parser.add_argument('--host', required=True, help='MongoDB host url')
args = parser.parse_args()

# Connect to MongoDB with username and password
client = MongoClient('mongodb+srv://' + args.username + ':' + args.password + '@' + args.host)
#'@cluster0.k9cz1z9.mongodb.net/?retryWrites=true&w=majority')
db = client['packet_db']
collection = db['packets']

# Set the network interface and filter for capturing packets
iface = 'wlp110s0'
filter = 'tcp'

# Define a packet handling function
def handle_packet(packet):

    # Convert packet to dictionary
    src_ip = packet[IP].src
    dst_ip = packet[IP].dst
    src_port = packet[TCP].sport
    dst_port = packet[TCP].dport
    timestamp = packet.time


    # Convert packet to dictionary
    packet_dict = {
        'src_ip': src_ip,
        'dst_ip': dst_ip,
        'src_port': src_port,
        'dst_port': dst_port,
        'timestamp': timestamp
    }
    # Insert packet into MongoDB
    print(packet_dict)
    collection.insert_one(packet_dict)

# Start sniffing packets indefinitely
while True:
    sniff(iface=iface, filter=filter, prn=handle_packet)

# Close the MongoDB connection
client.close()
