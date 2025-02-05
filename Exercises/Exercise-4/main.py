from pathlib import Path
import csv
import json


def flatten_json(json, parent_key='', sep='_'):
    flattened_dict = {}

    def _flatten(obj, key_prefix=''):
        if isinstance(obj, dict):
            for k, v in obj.items():
                _flatten(v, f"{key_prefix}{sep}{k}" if key_prefix else k)
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                _flatten(v, f"{key_prefix}{sep}{i}" if key_prefix else str(i))
        else:
            flattened_dict[key_prefix] = obj

    _flatten(json)
    return flattened_dict


def main():
    data_directory = Path(__file__).parent / "data"
    json_files = [str(file) for file in data_directory.rglob("*.json")]
    result_directory = Path(__file__).parent / "result"
    result_directory.mkdir(exist_ok=True)

    for file in json_files:
        with open(file) as json_file:
            json_data = json.load(json_file)
        json_file_name = file.split('\\')[-1]

        if isinstance(json_data, dict):
            json_data = [json_data]
        else:
            raise Exception("Json does not correspond to a dict")

        flattened_data = [flatten_json(entry) for entry in json_data]

        if flattened_data:
            csv_file_path = result_directory / json_file_name \
                .replace(".json", ".csv")
            with open(csv_file_path, "w", newline='') as csv_file:
                headers = sorted(set(key
                                     for row in flattened_data
                                     for key in row.keys()
                                     ))
                writer = csv.DictWriter(csv_file, fieldnames=headers)
                writer.writeheader()
                writer.writerows(flattened_data)


if __name__ == "__main__":
    main()
