import os
import json
from tqdm import tqdm
import time
# Define the base directory where the json files are located
base_dir = "/home/guangyi.liu/shared-folder/datasets/guangyi_data/scripts/panda70m/"

# Initialize a dictionary to hold the combined data


# Function to recursively search for all files ending with 'captions.json' in a directory
def find_and_combine_json_files(directory):
    # Iterate through each item in the directory
    for item in tqdm(os.listdir(directory), ncols=90, disable=True):
        item_path = os.path.join(directory, item)
        # Check if the item is a directory, if so, recursively search within it
        if os.path.isdir(item_path):
            find_and_combine_json_files(item_path)
        elif item_path.endswith('gemini_captions_v4_debug.json'):
            # Found a json file, read and combine its content
            with open(item_path, 'r') as json_file:
                # Initialize an empty dictionary for this file
                file_dict = {}
                for line in json_file:
                    # Each line is a separate dict; merge it into file_dict
                    line_dict = json.loads(line)
                    file_dict.update(line_dict)
                # Add the combined dictionary of this file to the main dictionary
                combined_dict[item_path] = file_dict

# Start the process
while(1):
    combined_dict = {}
    find_and_combine_json_files(base_dir)
    total_length = sum(len(value) for value in combined_dict.values())
    print(total_length)
    if total_length > 50000:
        os.system("pkill -f 'python -u caption' ")
        print('killed')
        time.sleep(3)
        os.system("pkill -f 'python -u caption' ")
        exit()
    time.sleep(60)

#python /lustre/scratch/users/guangyi.liu/repo/video_process/stop_gpt_caption.py