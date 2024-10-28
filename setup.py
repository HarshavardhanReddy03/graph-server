import os


def create_data_folders():
    folders = [
        "data/livestate",
        "data/statearchive",
        "data/schemaarchive",
        "data/liveschema",
    ]

    for folder in folders:
        os.makedirs(folder, exist_ok=True)
        print(f"Created folder: {folder}")


if __name__ == "__main__":
    create_data_folders()
