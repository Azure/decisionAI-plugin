import csv

def save_to_csv(data, path):
    with open(path, 'w', newline="\n") as data_file:
        writer = csv.writer(data_file)
        writer.writerows(data)

def read_from_csv(path):
    with open(path, 'r') as data_file:
        reader = csv.reader(data_file)
        data = [line for line in reader]
    return data