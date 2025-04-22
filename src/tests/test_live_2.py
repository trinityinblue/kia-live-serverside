import pytest
from datetime import datetime, timedelta
from google.transit import gtfs_realtime_pb2

from src.live_data_service.live_data_transformer import transform_response_to_feed_entities
from src.live_data_service.feed_entity_updater import update_feed_message
from src.shared import feed_message, feed_message_lock


@pytest.fixture
def sample_bmtc_api_data():
    """Simulates BMTC API 'up.data + down.data' merged."""
    now = datetime.now()
    trip_start = (now + timedelta(minutes=2)).strftime("%H:%M")  # Ensure within match window

    return [
        {
            "routeid": "1234",
            "stationid": "s1",
            "stationname": "Stop 1",
            "vehicleDetails": [
                {
                    "vehicleid": "v001",
                    "vehiclenumber": "KA01AB1234",
                    "sch_tripstarttime": trip_start,
                    "sch_arrivaltime": "11:10",
                    "sch_departuretime": "11:12",
                    "actual_arrivaltime": "11:11",
                    "actual_departuretime": "11:13",
                    "centerlat": 12.9716,
                    "centerlong": 77.5946,
                    "heading": 180
                }
            ]
        },
        {
            "routeid": "1234",
            "stationid": "s2",
            "stationname": "Stop 2",
            "vehicleDetails": [
                {
                    "vehicleid": "v001",
                    "vehiclenumber": "KA01AB1234",
                    "sch_tripstarttime": trip_start,
                    "sch_arrivaltime": "11:25",
                    "sch_departuretime": "11:27",
                    "actual_arrivaltime": "",
                    "actual_departuretime": "",
                    "centerlat": 12.9721,
                    "centerlong": 77.5950,
                    "heading": 182
                }
            ]
        }
    ]


def test_transformer_generates_valid_feed_entities(sample_bmtc_api_data):
    job = {
        "trip_id": "1234_1",
        "trip_time": datetime.now(),
        "route_id": "1234",
        "parent_id": 5678
    }

    entities = transform_response_to_feed_entities(sample_bmtc_api_data, job)

    assert len(entities) == 1
    entity = entities[0]
    assert entity.id == "veh_v001"
    assert entity.trip_update.trip.trip_id == "1234_1"
    assert len(entity.trip_update.stop_time_update) == 2

    stu1 = entity.trip_update.stop_time_update[0]
    assert stu1.stop_id == "s1"
    assert stu1.arrival.time > 0
    assert stu1.arrival.delay == 60  # 1 minute delay

    stu2 = entity.trip_update.stop_time_update[1]
    assert stu2.stop_id == "s2"
    assert stu2.arrival.time > 0
    assert stu2.arrival.delay == 0  # no actual time provided


def test_feed_message_updated_correctly(sample_bmtc_api_data):
    job = {
        "trip_id": "1234_1",
        "trip_time": datetime.now(),
        "route_id": "1234",
        "parent_id": 5678
    }

    entities = transform_response_to_feed_entities(sample_bmtc_api_data, job)

    update_feed_message(entities)

    with feed_message_lock:
        assert feed_message.header.gtfs_realtime_version == "2.0"
        assert feed_message.header.timestamp > 0
        assert len(feed_message.entity) == 1
        assert feed_message.entity[0].id == "veh_v001"
