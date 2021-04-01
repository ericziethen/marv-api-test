

import argparse
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from my_marvel import marvel_requests


def main():
    """Run the main function."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-puk', '--public_key', help='The Public Key', required=True)
    parser.add_argument('-prk', '--private_key', help='The Private Key.', required=True)
    parser.add_argument('-dd', '--dest_dir', help='The Directory to store the data.', required=True)
    parser.add_argument('-lf', '--log_file_path', help='The Log File path.', required=True)
    args = parser.parse_args()

    if (not os.path.exists(args.dest_dir)) or (not os.path.isdir(args.dest_dir)):
        raise ValueError(F'"{args.dest_dir}" is not a vaid directory')

    marvel_requests.get_data(log_path=args.log_file_path, public_key=args.public_key,
                             private_key=args.private_key, target_dir=args.dest_dir)


if __name__ == '__main__':
    main()
