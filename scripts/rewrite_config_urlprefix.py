"""Rewrite sql.js-httpvfs config JSON so chunk URLs point at an external host.

Used by the dashboard workflow after uploading query DB chunks to R2:
takes each queries_*_config.json in docs/data/, prepends the R2 public
base URL to the urlPrefix field so the browser fetches chunks from R2
instead of GH Pages. urlPrefix values that are already absolute
(http:// or https://) are left alone.

Usage:
    R2_PUBLIC_URL=https://pub-XYZ.r2.dev \
        python scripts/rewrite_config_urlprefix.py docs/data/queries_*_config.json
"""
import json
import os
import sys


def main(paths):
    prefix_url = os.environ['R2_PUBLIC_URL'].rstrip('/') + '/data/'
    for path in paths:
        with open(path) as f:
            config = json.load(f)
        current = config.get('urlPrefix', '')
        if current.startswith(('http://', 'https://')):
            print(f'{path}: urlPrefix already absolute, skipping')
            continue
        config['urlPrefix'] = prefix_url + current
        with open(path, 'w') as f:
            json.dump(config, f, separators=(',', ':'))
        print(f'{path}: urlPrefix = {config["urlPrefix"]}')


if __name__ == '__main__':
    main(sys.argv[1:])
