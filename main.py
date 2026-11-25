#!/usr/bin/env python3
"""兼容性别名：等价于 ``python3 backend.py``。"""
from backend import *  # noqa: F401,F403
from backend import main

if __name__ == "__main__":
    main()
