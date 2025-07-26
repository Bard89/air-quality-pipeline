#!/usr/bin/env python3
import sys
from src.application.cli import main
import asyncio

if __name__ == "__main__":
    asyncio.run(main())