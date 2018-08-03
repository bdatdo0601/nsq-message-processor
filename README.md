# Wistia Message Processor Challenge

## Pre-requirement

- [Ruby](https://www.ruby-lang.org/en/) - Local nsq cluster
- [Ruby Bundler](https://bundler.io/) - Ruby gem dependencies manager
- [Python 2.7](https://www.python.org/) - NSQ processor
- [Python 3.7](https://www.python.org/) - For script that will send async requests of guid messages to a given endpoints
        - [asyncio](https://docs.python.org/3/library/asyncio.html) - Asynchronous I/O
        - [aiohttp](https://aiohttp.readthedocs.io/) - Allow making async http requests
- [Pipenv](https://docs.pipenv.org/) - Python dependencies manager

## Setup

1. Install **Ruby**'s dependencies using `Bundler`: `bundle install`
2. Install **Python**'s dependencies using `Pipenv`: `pipenv install`

## Execution

1. execute `main.py` with **python 2.7** to run an instance of NSQ processor with NSQ cluster:
    ```bash
    python main.py <amount of desired nsqd nodes> <amount of desired nsqlookupd nodes>
    ```
2. Once excuted, the instance will response with a list of address into shell output, this will be addresses for incoming requests
3. To emulate 100000 requst, use **python 3.7** to execute `datagenerator.py`
    ```bash
    python3 datagenerator.py <endpoints-to-send-requests-to(ex: 127.0.0.1:4251)>
    ```
4. The output will be printed both into shell output and also to `output.txt`

## Testing

To test the functionality of the whole flow, simply execute `main.test.py` with **Python 2.7**.
The script will instantiate a processor instance, send a few request to it based on a guid dictionary.
It will then gather the response to recompose the dictionary and compare it against the original data.

```bash
python main.test.py
```