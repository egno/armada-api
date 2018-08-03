# Armada API

REST API for integration between external services:

- Billing database (site http://armada.land)
- Accounting (1C)
- SMS and email sending (http://unisender.com)

## How to start via docker

Fill configuration variables in config.py

    docker build -t armada-api .

    docker run -d --restart=always --net=host --name=armada-api armada-api
