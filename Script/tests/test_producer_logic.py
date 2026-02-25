import pytest
from producer.data_generator import generate_heart_rate, generate_patient_event


def test_generate_heart_rate_normal(monkeypatch):
    monkeypatch.setattr("producer.data_generator.random.random", lambda: 1.0)

    result = generate_heart_rate()

    assert 60 <= result["heart_rate"] <= 100
    assert result["status"] == "normal"


def test_generate_heart_rate_anomaly(monkeypatch):
    monkeypatch.setattr("producer.data_generator.random.random", lambda: 0.0)
    monkeypatch.setattr("producer.data_generator.random.choice", lambda x: "high")

    result = generate_heart_rate()

    assert result["status"] == "tachycardia"
    assert result["heart_rate"] >= 120


def test_generate_patient_event_structure():
    event = generate_patient_event(10)

    assert event["patient_id"] == 10
    assert "timestamp" in event
    assert "heart_rate" in event
    assert "status" in event