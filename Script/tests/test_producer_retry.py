from unittest.mock import MagicMock
from producer.data_generator import send_with_retry


def test_send_success():
    mock_producer = MagicMock()

    mock_future = MagicMock()
    mock_metadata = MagicMock()
    mock_metadata.partition = 0
    mock_future.get.return_value = mock_metadata

    mock_producer.send.return_value = mock_future

    event = {
        "patient_id": 1,
        "heart_rate": 80,
        "status": "normal"
    }

    send_with_retry(mock_producer, event)

    mock_producer.send.assert_called_once()
    mock_future.get.assert_called_once()