from src.local_file_service.gtfs_builder import build_stops, build_trips_and_stop_times

def test_build_stops_deduplication():
    sample_stops = {
        "Route A": {
            "stops": [
                {"name": "Stop A", "name_kn": "ಸ್ಟಾಪ್ A", "loc": [12.0, 77.0], "stop_id": "S1"},
                {"name": "Stop B", "name_kn": "ಸ್ಟಾಪ್ B", "loc": [13.0, 78.0]},
                {"name": "Stop A", "name_kn": "ಸ್ಟಾಪ್ A", "loc": [12.0, 77.0]}
            ]
        }
    }
    stops, stop_id_map, translations = build_stops(sample_stops)
    assert len(stops) == 2
    assert any(t["translation"] == "ಸ್ಟಾಪ್ A" for t in translations)
    assert len(stop_id_map) == 2

def test_build_trips_and_stop_times_id_generation():
    client_stops = {
        "R1": {
            "stops": [
                {"name": "A", "name_kn": "ಎ", "loc": [10.0, 20.0], "distance": 0, "stop_id": "s1"},
                {"name": "B", "name_kn": "ಬಿ", "loc": [10.5, 20.5], "distance": 10, "stop_id": "s2"}
            ]
        }
    }
    start_times = {
        "R1": [{"start": 300, "duration": 60}]
    }

    times_data = {}
    routes_children = {"R1": '123'}
    routes_shapes_map = {
        '123': 's123'
    }
    stop_id_map = {
        (10.0, 20.0, "A"): "s1",
        (10.5, 20.5, "B"): "s2"
    }

    trips, stop_times, translations = build_trips_and_stop_times(
        client_stops, start_times, times_data, routes_children, stop_id_map, routes_shapes_map
    )

    assert trips[0]["trip_id"] == "123_1"
    assert stop_times[0]["departure_time"] == "03:00:10"
    assert stop_times[0]["arrival_time"] == "03:00:00"
    assert stop_times[-1]["stop_id"] == "s2"
    assert len(translations) == 1

def test_date_time_change():
    client_stops = {
        "R1": {
            "stops": [
                {"name": "A", "name_kn": "ಎ", "loc": [10.0, 20.0], "distance": 0, "stop_id": "s1"},
                {"name": "B", "name_kn": "ಬಿ", "loc": [10.5, 20.5], "distance": 20, "stop_id": "s2"}
            ]
        }
    }
    start_times = {
        "R1": [{"start": 2350, "duration": 60}]
    }

    times_data = {}
    routes_children = {"R1": '123'}
    stop_id_map = {
        (10.0, 20.0, "A"): "s1",
        (10.5, 20.5, "B"): "s2"
    }
    routes_shapes_map = {
        '123': 's123'
    }

    trips, stop_times, translations = build_trips_and_stop_times(
        client_stops, start_times, times_data, routes_children, stop_id_map, routes_shapes_map
    )

    assert trips[0]["trip_id"] == "123_1"
    assert stop_times[0]["departure_time"] == "23:50:10"
    assert stop_times[-1]["arrival_time"] == "24:50:00"
    assert stop_times[-1]["stop_id"] == "s2"
    assert len(translations) == 1
