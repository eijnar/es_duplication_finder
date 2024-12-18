# Duplication finder

## Introduction
During the aftermath of an incident we started seeing lots of duplicates. This tool helps us search all _id and look for them.
It uses redis as a cache since holding more than 60 million individual _ids is very demanding.


## Installation

What external tools are needed?

- Redis

### Redis

The way that the script is designed it needs a local redis server running. Running this in WSL2 using Debian you can quickly install this and have it running with just a few commands:

```
# sudo apt install redis-server
# sudo systemctl start redis-server
```

### VirtualEnv and PIP

Best way to run the script is in an virtual environment, and install the dependencies in this virutal environment. The latest performance improvements in python also helps. The script was written and tested using python 3.11

```
# python -m venv .venv
# source .venv/bin/activate
# pip install -r requirements.txt
```

## Configuration

### config.json

This is the main configuration file. 

```json
{
  "elasticsearch": {
    "hosts": ["http://localhost:9200"], // can be a list
    "api_key": "API_KEY_IN_BASE64_FORMAT" // generate one from kibana
  },
  "output_file": "duplicates.csv",
  "aliases": ["duplicates-alias"], // can be a list
  "page_size": 10000, // number of documents fetched each batch
  "scroll_time": "2m" // how long the session is open, if each batch takes longer to fetch, you need to change this
}
```

## Running

The main program is started like so:
```
# python main.py
```

## Logging

There will be a log file created. Logging is sparse at the time of writing