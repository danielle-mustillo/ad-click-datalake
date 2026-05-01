#!/bin/bash

curl --location 'http://localhost:9337/click' \
--header 'Content-Type: application/json' \
--data '{
    "eventId": "123",
    "userId": "abc",
    "adId": "ad789",
    "campaignId": "campaign123",
    "country": "CA",
    "device": "mobile",
    "eventTime": "2026-05-01T10:00:00Z"
}'
