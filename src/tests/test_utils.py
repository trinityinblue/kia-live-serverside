from src.shared.utils import interpolate_trip_times, decode_polyline, data_has_changed

def test_interpolate_trip_times():
    stops = [
        ("stop1", 0, "Stop 1"),
        ("stop2", 10, "Stop 2"),
        ("stop3", 20, "Stop 3")
    ]
    times = interpolate_trip_times(start_time=300, total_duration=60, stops=stops)
    assert times == [300, 330, 400]

def test_decode_polyline():
    # This polyline represents [(38.5, -120.2), (40.7, -120.95), (43.252, -126.453)]
    encoded = "_p~iF~ps|U_ulLnnqC_mqNvxq`@"
    decoded = decode_polyline(encoded)
    assert decoded == [(-120.2, 38.5), (-120.95, 40.7), (-126.453, 43.252)]

def test_data_has_changed():
    old_data = {
        "stops.txt": [{"stop_id": "s1", "stop_name": "A"}],
        "routes.txt": [{"route_id": "r1", "route_short_name": "X"}]
    }
    new_data_same = {
        "stops.txt": [{"stop_id": "s1", "stop_name": "A"}],
        "routes.txt": [{"route_id": "r1", "route_short_name": "X"}]
    }
    new_data_diff = {
        "stops.txt": [{"stop_id": "s1", "stop_name": "A"}],
        "routes.txt": [{"route_id": "r1", "route_short_name": "Y"}]
    }

    assert data_has_changed(new_data_same, old_data) is False
    assert data_has_changed(new_data_diff, old_data) is True
