"""Tests for Pet Activity Enjoyment flows."""

import pytest
from unittest.mock import patch, MagicMock

from flows.pet_activity_enjoyment import (
    pet_event_ingestion,
    validate_data_quality,
    aggregate_pet_metrics,
)


class TestPetEventIngestion:
    """Tests for the pet_event_ingestion task."""

    @patch("flows.pet_activity_enjoyment.subprocess.run")
    def test_successful_ingestion(self, mock_run):
        """Test successful Spark job submission."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Job completed successfully",
            stderr="",
        )

        result = pet_event_ingestion.fn()

        assert result["success"] is True
        assert result["returncode"] == 0

    @patch("flows.pet_activity_enjoyment.subprocess.run")
    def test_failed_ingestion(self, mock_run):
        """Test failed Spark job submission."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Spark job failed",
        )

        result = pet_event_ingestion.fn()

        assert result["success"] is False
        assert result["returncode"] == 1


class TestValidateDataQuality:
    """Tests for the validate_data_quality task."""

    def test_validation_returns_result(self):
        """Test that validation returns expected structure."""
        result = validate_data_quality.fn()

        assert "checks_passed" in result
        assert "total_checks" in result
        assert "failed_checks" in result


class TestAggregatePetMetrics:
    """Tests for the aggregate_pet_metrics task."""

    def test_aggregation_with_successful_ingestion(self):
        """Test aggregation when ingestion succeeded."""
        ingestion_result = {"success": True}

        result = aggregate_pet_metrics.fn(ingestion_result)

        assert result["success"] is True

    def test_aggregation_with_failed_ingestion(self):
        """Test aggregation when ingestion failed."""
        ingestion_result = {"success": False}

        result = aggregate_pet_metrics.fn(ingestion_result)

        assert result["success"] is False
        assert "error" in result
