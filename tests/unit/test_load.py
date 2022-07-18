#!/usr/bin/env pytest
import pytest

from digital_land.phase.load import LoadPhase, Stream


def test_stream_constructor_needs_path_or_f():
    with pytest.raises(TypeError):
        Stream()


def test_stream_constructor_attributes():
    resource = "0001a1baf9ddd7505cfef2e671292122de73a44299e5f5e584e9ec1514c0181c"
    path = "/tmp/" + resource

    with open(path, "w") as f:
        stream = Stream(path=path)
        assert stream.path == path
        assert stream.resource == resource

        resource = "test"
        stream = Stream(path=path, resource=resource, dataset="test-dataset")
        assert stream.path == path
        assert stream.resource == resource
        assert stream.dataset == "test-dataset"

        resource = "test"
        stream = Stream(f=f)
        assert stream.f == f


def test_load_constructor_attributes():
    resource = "0001a1baf9ddd7505cfef2e671292122de73a44299e5f5e584e9ec1514c0181c"
    path = "/tmp/" + resource

    with open(path, "w") as f:
        stream = LoadPhase(path=path).stream
        assert stream.path == path
        assert stream.resource == resource

        resource = "test"
        stream = LoadPhase(path=path, resource=resource, dataset="test-dataset").stream
        assert stream.path == path
        assert stream.resource == resource
        assert stream.dataset == "test-dataset"

        resource = "test"
        stream = LoadPhase(f=f).stream
        assert stream.f == f
