#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the Flask app
from main import app

# Handler for Vercel
def handler(request, context):
    return app(request, context)