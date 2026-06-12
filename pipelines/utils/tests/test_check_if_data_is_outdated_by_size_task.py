from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from pipelines.utils.metadata.tasks import check_if_data_is_outdated_by_size


@patch("pipelines.utils.metadata.tasks.get_redis_client")
@patch("pipelines.utils.metadata.tasks.log")
def test_first_run(mock_log, mock_get_redis_client):
    # Setup mock
    mock_redis = MagicMock()
    mock_redis.get.return_value = None
    mock_get_redis_client.return_value = mock_redis

    dataset_id = "test_dataset"
    table_id = "test_table"
    byte_length = 1000

    # Execute
    result = check_if_data_is_outdated_by_size.run(
        dataset_id, table_id, byte_length
    )

    # Verify
    assert result is True
    mock_redis.set.assert_called_once()
    args, _ = mock_redis.set.call_args
    assert args[0] == dataset_id
    assert table_id in args[1]
    today = datetime.today().strftime("%Y-%m-%d")
    assert args[1][table_id][today] == byte_length


@patch("pipelines.utils.metadata.tasks.get_redis_client")
@patch("pipelines.utils.metadata.tasks.log")
def test_size_increased(mock_log, mock_get_redis_client):
    # Setup mock
    mock_redis = MagicMock()
    mock_redis.get.return_value = {"test_table": {"2023-01-01": 500}}
    mock_get_redis_client.return_value = mock_redis

    dataset_id = "test_dataset"
    table_id = "test_table"
    byte_length = 1000

    # Execute
    result = check_if_data_is_outdated_by_size.run(
        dataset_id, table_id, byte_length
    )

    # Verify
    assert result is True
    mock_redis.set.assert_called_once()
    args, _ = mock_redis.set.call_args
    assert args[1][table_id]["2023-01-01"] == 500
    today = datetime.today().strftime("%Y-%m-%d")
    assert args[1][table_id][today] == byte_length


@patch("pipelines.utils.metadata.tasks.get_redis_client")
@patch("pipelines.utils.metadata.tasks.log")
def test_size_equal(mock_log, mock_get_redis_client):
    # Setup mock
    mock_redis = MagicMock()
    mock_redis.get.return_value = {"test_table": {"2023-01-01": 1000}}
    mock_get_redis_client.return_value = mock_redis

    dataset_id = "test_dataset"
    table_id = "test_table"
    byte_length = 1000

    # Execute
    result = check_if_data_is_outdated_by_size.run(
        dataset_id, table_id, byte_length
    )

    # Verify
    assert result is False
    mock_redis.set.assert_not_called()


@patch("pipelines.utils.metadata.tasks.get_redis_client")
@patch("pipelines.utils.metadata.tasks.log")
def test_size_decreased(mock_log, mock_get_redis_client):
    # Setup mock
    mock_redis = MagicMock()
    mock_redis.get.return_value = {"test_table": {"2023-01-01": 2000}}
    mock_get_redis_client.return_value = mock_redis

    dataset_id = "test_dataset"
    table_id = "test_table"
    byte_length = 1000

    # Execute & Verify
    with pytest.raises(ValueError):
        check_if_data_is_outdated_by_size.run(
            dataset_id, table_id, byte_length
        )


@patch("pipelines.utils.metadata.tasks.get_redis_client")
@patch("pipelines.utils.metadata.tasks.log")
def test_keep_last_10(mock_log, mock_get_redis_client):
    # Setup mock with 10 records
    existing_data = {f"2023-01-{i + 1:02d}": 100 + i for i in range(10)}
    mock_redis = MagicMock()
    mock_redis.get.return_value = {"test_table": existing_data}
    mock_get_redis_client.return_value = mock_redis

    dataset_id = "test_dataset"
    table_id = "test_table"
    byte_length = 1000

    # Execute
    result = check_if_data_is_outdated_by_size.run(
        dataset_id, table_id, byte_length
    )

    # Verify
    assert result is True
    mock_redis.set.assert_called_once()
    args, _ = mock_redis.set.call_args
    table_data = args[1][table_id]
    assert len(table_data) == 10
    today = datetime.today().strftime("%Y-%m-%d")
    assert today in table_data
    # 2023-01-01 should have been removed
    assert "2023-01-01" not in table_data
