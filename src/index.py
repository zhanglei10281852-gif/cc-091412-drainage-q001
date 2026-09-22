import os

from app import create_server


if __name__ == "__main__":
    server = create_server(data_dir=os.environ.get("DATA_DIR"))
    print("溢流调度服务已启动", flush=True)
    server.serve_forever()
