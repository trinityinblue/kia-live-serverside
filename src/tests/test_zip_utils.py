import os
import tempfile
from src.shared.utils import zip_gtfs_data, load_gtfs_zip

def test_zip_gtfs_data_and_load_gtfs_zip():
    gtfs_sample = {
        "stops.txt": [
            {"stop_id": "s1", "stop_name": "A", "stop_lat": "12.0", "stop_lon": "77.0"},
            {"stop_id": "s2", "stop_name": "B", "stop_lat": "13.0", "stop_lon": "78.0"},
        ],
        "routes.txt": [
            {"route_id": "r1", "route_short_name": "100", "route_long_name": "A to B", "route_type": "3"}
        ]
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "gtfs_test.zip")

        # Write sample GTFS data to zip
        zip_gtfs_data(gtfs_sample, zip_path)
        assert os.path.exists(zip_path)

        # Re-load the zip and check values
        loaded_data = load_gtfs_zip(zip_path)
        assert "stops.txt" in loaded_data
        assert "routes.txt" in loaded_data

        stops = loaded_data["stops.txt"]
        routes = loaded_data["routes.txt"]

        assert len(stops) == 2
        assert stops[0]["stop_id"] == "s1"
        assert stops[1]["stop_name"] == "B"

        assert routes[0]["route_id"] == "r1"
        assert routes[0]["route_long_name"] == "A to B"
