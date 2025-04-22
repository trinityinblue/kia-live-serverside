import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime, timedelta
from queue import PriorityQueue

from src.live_data_service.live_data_scheduler import populate_schedule
from src.live_data_service.live_data_getter import fetch_route_data


@pytest.fixture
def patched_state(monkeypatch):
    from src.shared import start_times, routes_children, routes_parent, scheduled_timings

    # Clear before test
    start_times.clear()
    routes_children.clear()
    routes_parent.clear()
    while not scheduled_timings.empty():
        scheduled_timings.get()

    # Setup test state
    start_times.update({
        "KIA-10 DOWN": [{"start": 450, "duration": 120}]
    })
    routes_children.update({
        "KIA-10 DOWN": 3813
    })
    routes_parent.update({
        "KIA-10 DOWN": 2124
    })

    yield {
        "start_times": start_times,
        "routes_children": routes_children,
        "routes_parent": routes_parent,
        "scheduled_timings": scheduled_timings
    }


def test_populate_schedule_adds_correct_entries(patched_state, monkeypatch):
    monkeypatch.setenv("KIA_QUERY_INTERVAL", "5")
    monkeypatch.setenv("KIA_QUERY_AMOUNT", "2")

    populate_schedule()
    from src.shared import scheduled_timings

    scheduled = []
    while not scheduled_timings.empty():
        scheduled.append(scheduled_timings.get())

    assert len(scheduled) == 5
    times = [s[0] for s in scheduled]
    metadata = [s[1] for s in scheduled]

    for meta in metadata:
        assert meta["trip_id"].startswith("3813_")
        assert meta["parent_id"] == 2124
        assert meta["route_id"] == "3813"
        assert isinstance(meta["trip_time"], datetime)


@pytest.mark.asyncio
async def test_fetch_route_data_success(monkeypatch):
    mock_json = {
        "issuccess": True,
        "up": {
            "data": [{"routeid": 2124, "stationid": "S1", "vehicleDetails": []}]
        },
        "down": {
            "data": [{"routeid": 2124, "stationid": "S2", "vehicleDetails": []}]
        }
    }

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value=mock_json)

    # Patch aiohttp.ClientSession to return a session with post() that returns the mock response
    mock_session = MagicMock()
    mock_session.post.return_value.__aenter__.return_value = mock_response

    monkeypatch.setattr("aiohttp.ClientSession.post", MagicMock(return_value=mock_session))

    result = await fetch_route_data(2124)
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["stationid"] == "S1"
    assert result[1]["stationid"] == "S2"
