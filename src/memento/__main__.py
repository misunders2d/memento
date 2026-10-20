import argparse
import os
import sys
from pathlib import Path



def main():
    if sys.argv[1:2] == ['agent']:
        from .runner import main as runner_main
        return runner_main(sys.argv[1:])
    parser = argparse.ArgumentParser(
        description='Memento immutable evidence service',
        epilog='Connect a native harness: memento agent add {pi,omp,claude,codex}')
    parser.add_argument('--data-dir', type=Path,
                        default=Path(os.environ.get('MEMENTO_DATA_DIR', './data')))
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8765)
    args = parser.parse_args()
    import uvicorn
    from .api import create_app

    uvicorn.run(create_app(args.data_dir), host=args.host, port=args.port, workers=1,
                access_log=False)


if __name__ == '__main__':
    raise SystemExit(main())
