import argparse
import os
from pathlib import Path

import uvicorn

from .api import create_app


def main():
    parser = argparse.ArgumentParser(description='Memento immutable evidence service')
    parser.add_argument('--data-dir', type=Path,
                        default=Path(os.environ.get('MEMENTO_DATA_DIR', './data')))
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8765)
    args = parser.parse_args()
    uvicorn.run(create_app(args.data_dir), host=args.host, port=args.port, workers=1,
                access_log=False)


if __name__ == '__main__':
    main()
