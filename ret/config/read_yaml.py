#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ret.loguru import logger
import yaml

def read_yaml(file_path):
    logger.debug(f'read_yaml:')
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

def main():
    file_path = 'windows_config.yaml'
    dict_ = read_yaml(file_path)


if __name__ == '__main__':
    main()
