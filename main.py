# Import necessary modules
import json
import zlib
import base64
from collections import defaultdict
from datetime import datetime
import boto3

# Function to load data from a JSON file
def load_data(file_path):
    with open(file_path) as f:
        data = json.load(f)
    return data

# Function to get a list of unique vehicle IDs
def get_unique_vehicle_ids(data):
    # CREATING A LIST OF ALL VEHICAL IDS. NOT UNIQUE
    vehicle_list = []
    for vehicle_id in data['records']:
        vehicle_list.append(vehicle_id)

    # CREATING A UNIQUE LIST OF OCCURANCE FOR 'vehicle_id' 
    unique_vehicle_id_list = list({item['vehicle_id'] for item in data['records']})
    print("\nQUESTION:\t How many vehicles are there in total?")
    print("ANSWER:\t\t The number of unique vehicles =", len(unique_vehicle_id_list), "\n")
    return unique_vehicle_id_list

#print(len(vehicle_list)) # this prints all vehicle_ids which accumilates to 1023

'''
###### ANOTHER METHOD, HOWEVER GREATER COMPLEXITY ######
vehicle_id_list = [] # creating an empty list to store unique vehicle_id's
counter = 0 # initialising a counter

for vehicle_id in vehicle_id_list:
    # print(vehicle_id) # prints all vehical_ids
    # conditional statement which appends to the empty list when it finds a unique vehical_id
    if vehicle_id not in vehicle_id_list:
          counter += 1
          vehicle_id_list.append(vehicle_id)

print(counter)

# Printing the list of vehicle_id on new lines for formatting 
for vehicle_ids in vehicle_id_list:
    print(vehicle_ids)

print(len(vehicle_id_list)) # this prints the number of elements in the list
'''


# Function to decode payloads and get vehicle speeds
def decode_payloads(data):
    vehicle_speeds_list = {}
    full_decoded_json = []

    # Iterate over each record
    for record in data['records']:
        vehicle_id = record['vehicle_id']
        payload_encoded = record['payload']

        # Decode the base64-encoded payload
        payload_compressed = base64.b64decode(payload_encoded)

        # Decompress the ZIP-compressed payload
        payload_decompressed = zlib.decompress(payload_compressed)

        # Variable whihc holds the fully decoded payload
        payload_decompressed_only = payload_decompressed.decode()
        payloads_load = json.loads(payload_decompressed_only)
        payload_dict = json.loads(payload_decompressed_only)
        speed = payloads_load['tracking'][0]['speed']

        # Create a new dictionary with the desired format from decoded_records_example
        record_dict = {
            "vehicle_id": vehicle_id,
            "payload": payload_dict
        }
        full_decoded_json.append(record_dict) # full_decoded_json is now the format I want

        # If the key (vehicle_id) is not in the dictionary, create a new list
        if vehicle_id not in vehicle_speeds_list:
            vehicle_speeds_list[vehicle_id] = []

        # Append the speed value to the list corresponding to the vehicle_id
        vehicle_speeds_list[vehicle_id].append(speed)

    return vehicle_speeds_list, full_decoded_json


# Function to find the vehicle with the highest average speed
# I initialize max_avg_speed and max_vehicle_id to keep track of the maximum average speed and its corresponding vehicle_id.
# I iterate over the dictionary items using vehicle_speeds.items().
# For each vehicle_id and speeds list, I filter out the zero speeds using a list comprehension non_zero_speeds = [speed for speed in speeds if speed > 0].
# If there are non-zero speeds, I calculate the average speed by summing the non-zero speeds and dividing by the length of non_zero_speeds.
# I compare the calculated average speed with the current max_avg_speed. If it's greater, I update max_avg_speed and max_vehicle_id.
# After iterating over all vehicle_ids, I print the max_vehicle_id and max_avg_speed.

def get_vehicle_with_highest_avg_speed(vehicle_speeds_list):
    max_avg_speed = 0
    max_vehicle_id = None

    for vehicle_id, speeds in vehicle_speeds_list.items():
        non_zero_speeds = [speed for speed in speeds if speed > 0]
        if non_zero_speeds:
            avg_speed = sum(non_zero_speeds) / len(non_zero_speeds)
            if avg_speed > max_avg_speed:
                max_avg_speed = avg_speed
                max_vehicle_id = vehicle_id

    print("QUESTION:\t Which vehicle drives the fastest on average?")
    print("ANSWER:\t\t", f"The vehicle_id with the highest average speed is: '{max_vehicle_id}' which has the highest average speed of {max_avg_speed}\n")


# Function to find the vehicle with the longest status (PARKED, IDLING, MOVING)
# Now I want to get a list of the PARKED, IDLING and MOVING vehicles. I first processes the input data and stores the last parked, idling, and moving timestamps for each vehicle 
# in a dictionary. Then, I loop through the dictionary to find the longest times for each status and stores the corresponding vehicle IDs and timestamps. 
# Finally, it prints the vehicle IDs and times for the longest parked, idling, and moving vehicles.

def get_vehicle_with_longest_status(full_decoded_json):
    vehicles = {} # Initialize an empty dictionary to store vehicle data

    # Loop through each vehicle's data in the input JSON
    for vehicle_data in full_decoded_json:
        # Get the vehicle ID
        vehicle_id = vehicle_data['vehicle_id']
        # Get the tracking data for the vehicle
        tracking_data = vehicle_data['payload']['tracking']

        # Loop through each data point in the tracking data
        for data_point in tracking_data:
            # Get the timestamp, ignition status, and speed from the data point
            timestamp = data_point['timestamp']
            ignition = data_point['ignition']
            speed = data_point['speed']

            # Determine the status (PARKED, IDLING, or MOVING) based on ignition and speed
            status = 'PARKED' if ignition == 0 and speed == 0 else 'IDLING' if ignition == 1 and speed == 0 else 'MOVING'

            # Convert the timestamp string to a datetime object
            timestamp_object = datetime.fromisoformat(timestamp.replace(',+00:00', '+00:00'))

            # If the vehicle ID is not in the dictionary, initialize its data
            if vehicle_id not in vehicles:
                vehicles[vehicle_id] = {'last_parked': None, 'last_idling': None, 'last_moving': None}

            if status == 'PARKED':
                vehicles[vehicle_id]['last_parked'] = timestamp_object
            elif status == 'IDLING':
                # Update the last parked, idling, or moving timestamp for the vehicle
                if not vehicles[vehicle_id]['last_idling'] or timestamp_object > vehicles[vehicle_id]['last_idling']:
                    vehicles[vehicle_id]['last_idling'] = timestamp_object
            elif status == 'MOVING':
                if not vehicles[vehicle_id]['last_moving'] or timestamp_object > vehicles[vehicle_id]['last_moving']:
                    vehicles[vehicle_id]['last_moving'] = timestamp_object


    # Initialize variables to keep track of the longest parked, idling, and moving times and vehicle IDs
    longest_parked_vehicle_id = None
    longest_parked_time = None

    longest_idling_vehicle_id = None
    longest_idling_time = None

    longest_moving_vehicle_id = None
    longest_moving_time = None

    # Loop through each vehicle's data in the dictionary
    for vehicle_id, vehicle_data in vehicles.items():
        last_parked_time = vehicle_data['last_parked']
        last_idling_time = vehicle_data['last_idling']
        last_moving_time = vehicle_data['last_moving']
        # Get the last parked, idling, and moving times for the vehicle

        if not longest_parked_time or (last_parked_time and last_parked_time < longest_parked_time):
            # Update the longest parked time and vehicle ID if a longer parked time is found
            longest_parked_time = last_parked_time
            longest_parked_vehicle_id = vehicle_id

        if last_idling_time and (not longest_idling_time or last_idling_time > longest_idling_time):
            # Update the longest idling time and vehicle ID if a longer idling time is found
            longest_idling_time = last_idling_time
            longest_idling_vehicle_id = vehicle_id

        if last_moving_time and (not longest_moving_time or last_moving_time > longest_moving_time):
            # Update the longest moving time and vehicle ID if a longer moving time is found
            longest_moving_time = last_moving_time
            longest_moving_vehicle_id = vehicle_id

    # Print the vehicle IDs and times for the longest parked, idling, and moving vehicles
    print("QUESTION:\t Which vehicle has been PARKED the longest?")
    print("ANSWER:\t\t "f"The vehicle with ID '{longest_parked_vehicle_id}' has been parked the longest, since '{longest_parked_time}'\n")
    print("QUESTION:\t Which vehicle has been IDLING the longest?")
    print("ANSWER:\t\t "f"The vehicle with ID '{longest_idling_vehicle_id}' has been idling the longest, since '{longest_idling_time}'\n")
    print("QUESTION:\t Which vehicle has been MOVING the longest?")
    print("ANSWER:\t\t "f"The vehicle with ID '{longest_moving_vehicle_id}' has been moving the longest, since '{longest_moving_time}'\n")


# Main function to call other functions
def main():
    data = load_data("function_input.json")
    get_unique_vehicle_ids(data)
    vehicle_speeds_list, full_decoded_json = decode_payloads(data)
    get_vehicle_with_highest_avg_speed(vehicle_speeds_list)
    get_vehicle_with_longest_status(full_decoded_json)

# Entry point of the program
if __name__ == "__main__":
    main()