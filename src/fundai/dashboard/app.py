import json


def run():
    with open("./tests/raw_data.txt", "r") as f:
        page = f.read()
    print(json.loads(page))


if __name__ == "__main__":
    run()
