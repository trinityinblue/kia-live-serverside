## KIA - Live Update Service 
#### Takes data from BMTC APIs to provide a semi-live data feed

- ### Setup:
This is expected to work on an AWS EC2 Instance, however it can run anywhere. To set it up first install all dependents
via the poetry package manager `poetry install` command. Once installed you can simply run it via 
`poetry run python main.py`. To expose it you can use an nginx reverse proxy, or alternatively modify the source code to
expose flask properly.

- ### Data:
Data is returned in the following JSON format
```json
{"<ROUTE_NO_WITH_DIRECTION>": 
  {
    "<KNOWN_STOP_NAME>": [
      {
        "currentStop": "<STOP_NAME>",
        "currentStopLocation": <STOP_ID>,
        "direction": "<UP | DOWN>",
        "lastKnownStop": "<KNOWN_STOP_NAME>",
        "lastStop": "<STOP_NAME>",
        "lat": <DECIMAL_NUMBER>,
        "long": <DECIMAL_NUMBER>,
        "refresh": "<DATE_TIME_STRING(DD-MM-YYYY hh:mm:ss)>",
        "regno": "<REGISTRATION_NUMBER>",
        "routeno": "<ROUTE_NO_WITH_DIRECTION>",
        "stopCovered": <INTERNAL_VAL>,
        "stopCoveredOriginal": <INTERNAL_VAL>
      }
    ]
  },
  "pollDate": "<DATE_TIME_ISO_FORMAT>"
}
```

So a sample would look like:
```json
{
  "KIA-5D DOWN": {
    "Ashoka Pillar": [
      {
        "currentStop": "Wilson Garden Police Station",
        "currentStopLocationId": 21281,
        "direction": "UP",
        "lastKnownStop": "Ashoka Pillar",
        "lastStop": "Ashoka Pillar",
        "lat": 12.945986,
        "long": 77.595497,
        "refresh": "11-12-2024 10:29:38",
        "regno": "KA57F1809",
        "routeno": "KIA-5D UP",
        "stopCovered": 0,
        "stopCoveredOriginal": 1
      }
    ],
    "Udipalya": [
      {
        "currentStop": "Brigade Meadows Kanakapura Road",
        "currentStopLocationId": 35891,
        "direction": "DOWN",
        "lastKnownStop": "Udipalya",
        "lastStop": "Udipalya",
        "lat": 12.85927,
        "long": 77.528435,
        "refresh": "11-12-2024 10:29:22",
        "regno": "KA57F1788",
        "routeno": "KIA-5D DOWN",
        "stopCovered": 0,
        "stopCoveredOriginal": 1
      }
    ],
    "pollDate": "2024-12-11T05:00:36.716276"
  }
}
```
You will notice in the above example that even though it is for `KIA-5D DOWN` there is a value with the direction `UP`
This is because of inaccurate data received from BMTC API.