#!/usr/bin/env python
# coding: utf-8

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock, call
from click.testing import CliRunner
import ingest_green_data
import ingest_yellow_data


@pytest.fixture
def sample_dataframe():
    """Create a sample dataframe similar to taxi data."""
    return pd.DataFrame({
        'VendorID': [1, 2, 1],
        'lpep_pickup_datetime': pd.to_datetime(['2025-11-01', '2025-11-02', '2025-11-03']),
        'lpep_dropoff_datetime': pd.to_datetime(['2025-11-01', '2025-11-02', '2025-11-03']),
        'passenger_count': [1, 2, 3],
        'trip_distance': [1.5, 2.3, 0.8],
        'fare_amount': [10.0, 15.5, 7.0]
    })


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine."""
    engine = Mock()
    return engine


@pytest.fixture
def mock_inspector():
    """Create a mock SQLAlchemy inspector."""
    inspector = Mock()
    return inspector


class TestIngestGreenData:
    """Tests for ingest_green_data.py"""
    
    @patch('ingest_green_data.pd.read_parquet')
    @patch('ingest_green_data.create_engine')
    @patch('ingest_green_data.inspect')
    def test_ingest_new_table(self, mock_inspect, mock_create_engine, mock_read_parquet, 
                              sample_dataframe, mock_engine, mock_inspector):
        """Test 1: Should successfully ingest data into a new PostgreSQL table when the table does not exist."""
        # Arrange
        runner = CliRunner()
        mock_read_parquet.return_value = sample_dataframe
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        mock_inspector.has_table.return_value = False  # Table does not exist
        
        # Mock the to_sql method
        sample_dataframe.to_sql = Mock()
        
        # Act
        result = runner.invoke(ingest_green_data.run, [
            '--pg_user', 'test_user',
            '--pg_pass', 'test_pass',
            '--pg_host', 'test_host',
            '--pg_port', '5432',
            '--pg_db', 'test_db',
            '--year', '2025',
            '--month', '11',
            '--target_table', 'green_taxi_data'
        ])
        
        # Assert
        assert result.exit_code == 0
        mock_create_engine.assert_called_once_with('postgresql://test_user:test_pass@test_host:5432/test_db')
        mock_read_parquet.assert_called_once_with('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet')
        mock_inspector.has_table.assert_called_once_with('green_taxi_data')
        
        # Verify to_sql was called with replace mode for new table
        sample_dataframe.to_sql.assert_called_once_with(
            name='green_taxi_data',
            con=mock_engine,
            if_exists='replace',
            index=False
        )
        assert "Creating new table 'green_taxi_data'" in result.output
        assert "Done!" in result.output
    
    @patch('ingest_green_data.pd.read_parquet')
    @patch('ingest_green_data.create_engine')
    @patch('ingest_green_data.inspect')
    def test_append_existing_table(self, mock_inspect, mock_create_engine, mock_read_parquet,
                                   sample_dataframe, mock_engine, mock_inspector):
        """Test 2: Should successfully append data to an existing PostgreSQL table."""
        # Arrange
        runner = CliRunner()
        mock_read_parquet.return_value = sample_dataframe
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        mock_inspector.has_table.return_value = True  # Table exists
        
        # Mock the to_sql method on DataFrame chunks
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            # Act
            result = runner.invoke(ingest_green_data.run, [
                '--pg_user', 'test_user',
                '--pg_pass', 'test_pass',
                '--pg_host', 'test_host',
                '--pg_port', '5432',
                '--pg_db', 'test_db',
                '--year', '2025',
                '--month', '11',
                '--target_table', 'green_taxi_data'
            ])
            
            # Assert
            assert result.exit_code == 0
            mock_create_engine.assert_called_once_with('postgresql://test_user:test_pass@test_host:5432/test_db')
            mock_read_parquet.assert_called_once()
            mock_inspector.has_table.assert_called_once_with('green_taxi_data')
            
            # Verify to_sql was called with append mode
            assert mock_to_sql.called
            # Check that all calls used 'append' mode
            for call_args in mock_to_sql.call_args_list:
                assert call_args[1]['if_exists'] == 'append'
                assert call_args[1]['name'] == 'green_taxi_data'
                assert call_args[1]['con'] == mock_engine
                assert call_args[1]['index'] is False
            
            assert "Appending to existing table 'green_taxi_data'" in result.output
            assert "Done!" in result.output


class TestIngestYellowData:
    """Tests for ingest_yellow_data.py"""
    
    @patch('ingest_yellow_data.pd.read_parquet')
    @patch('ingest_yellow_data.create_engine')
    @patch('ingest_yellow_data.inspect')
    def test_ingest_new_table(self, mock_inspect, mock_create_engine, mock_read_parquet,
                              sample_dataframe, mock_engine, mock_inspector):
        """Test 3: Should successfully ingest data into a new PostgreSQL table when the table does not exist."""
        # Arrange
        runner = CliRunner()
        mock_read_parquet.return_value = sample_dataframe
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        mock_inspector.has_table.return_value = False  # Table does not exist
        
        # Mock the to_sql method
        sample_dataframe.to_sql = Mock()
        
        # Act
        result = runner.invoke(ingest_yellow_data.run, [
            '--pg_user', 'test_user',
            '--pg_pass', 'test_pass',
            '--pg_host', 'test_host',
            '--pg_port', '5432',
            '--pg_db', 'test_db',
            '--year', '2025',
            '--month', '11',
            '--target_table', 'yellow_taxi_data'
        ])
        
        # Assert
        assert result.exit_code == 0
        mock_create_engine.assert_called_once_with('postgresql://test_user:test_pass@test_host:5432/test_db')
        mock_read_parquet.assert_called_once_with('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet')
        mock_inspector.has_table.assert_called_once_with('yellow_taxi_data')
        
        # Verify to_sql was called with replace mode for new table
        sample_dataframe.to_sql.assert_called_once_with(
            name='yellow_taxi_data',
            con=mock_engine,
            if_exists='replace',
            index=False
        )
        assert "Creating new table 'yellow_taxi_data'" in result.output
        assert "Done!" in result.output
    
    @patch('ingest_yellow_data.pd.read_parquet')
    @patch('ingest_yellow_data.create_engine')
    @patch('ingest_yellow_data.inspect')
    def test_append_existing_table(self, mock_inspect, mock_create_engine, mock_read_parquet,
                                   sample_dataframe, mock_engine, mock_inspector):
        """Test 4: Should successfully append data to an existing PostgreSQL table."""
        # Arrange
        runner = CliRunner()
        mock_read_parquet.return_value = sample_dataframe
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        mock_inspector.has_table.return_value = True  # Table exists
        
        # Mock the to_sql method on DataFrame chunks
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            # Act
            result = runner.invoke(ingest_yellow_data.run, [
                '--pg_user', 'test_user',
                '--pg_pass', 'test_pass',
                '--pg_host', 'test_host',
                '--pg_port', '5432',
                '--pg_db', 'test_db',
                '--year', '2025',
                '--month', '11',
                '--target_table', 'yellow_taxi_data'
            ])
            
            # Assert
            assert result.exit_code == 0
            mock_create_engine.assert_called_once_with('postgresql://test_user:test_pass@test_host:5432/test_db')
            mock_read_parquet.assert_called_once()
            mock_inspector.has_table.assert_called_once_with('yellow_taxi_data')
            
            # Verify to_sql was called with append mode
            assert mock_to_sql.called
            # Check that all calls used 'append' mode
            for call_args in mock_to_sql.call_args_list:
                assert call_args[1]['if_exists'] == 'append'
                assert call_args[1]['name'] == 'yellow_taxi_data'
                assert call_args[1]['con'] == mock_engine
                assert call_args[1]['index'] is False
            
            assert "Appending to existing table 'yellow_taxi_data'" in result.output
            assert "Done!" in result.output
