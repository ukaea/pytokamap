import pytest
import dask
from dask import delayed
from dask.local import get_sync

from pytokamap.mast import MASTClient
from pytokamap.plugins import LoadUDA, UDAFormat, UDAType

try:
    import pyuda

    uda_available = True
except ImportError:
    uda_available = False


def _forked_uda_client(*args, type: str = "signal"):
    client = pyuda.Client()
    if type == "signal":
        return client.get(*args)
    else:
        return client.get_images(*args)


@pytest.mark.skipif(not uda_available, reason="UDA is not available")
def test_get_mast_client():
    client = MASTClient()
    client.get_image(30420, "rba")
    client.get_signal(30420, "/XSX/TCAM/1", "IDA")
    client.get_signal(30420, "/XSX/TCAM/2", "IDA")
    client.get_signal(30420, "/XSX/TCAM/3", "IDA")


@pytest.mark.skipif(not uda_available, reason="UDA is not available")
def test_get_mast_delayed():
    client = MASTClient()
    t1 = delayed(client.get_image)(30420, "rba")
    t2 = delayed(client.get_signal)(30420, "/XSX/TCAM/1", "IDA")
    t3 = delayed(client.get_signal)(30420, "/XSX/TCAM/2", "IDA")
    t4 = delayed(client.get_signal)(30420, "/XSX/TCAM/3", "IDA")
    dask.compute(t1, t2, t3, t4)


@pytest.mark.skipif(not uda_available, reason="UDA is not available")
def test_get_uda_delayed_synced():
    client = pyuda.Client()
    t1 = delayed(client.get_images)("rba", 30420)
    t2 = delayed(client.get)("/XSX/TCAM/1", 30420)
    t3 = delayed(client.get)("/XSX/TCAM/2", 30420)
    t4 = delayed(client.get)("/XSX/TCAM/3", 30420)

    dask.compute(t1, t2, t3, t4, schedular=get_sync)


@pytest.mark.skipif(not uda_available, reason="UDA is not available")
def test_get_uda_delayed_forked_client():
    t1 = delayed(_forked_uda_client)("rba", 30420, type="image")
    t2 = delayed(_forked_uda_client)("/XSX/TCAM/1", 30420)
    t3 = delayed(_forked_uda_client)("/XSX/TCAM/2", 30420)
    t4 = delayed(_forked_uda_client)("/XSX/TCAM/3", 30420)

    dask.compute(t1, t2, t3, t4)


@pytest.mark.skipif(not uda_available, reason="UDA is not available")
def test_get_uda_delayed_forked_client_synced():
    t1 = delayed(_forked_uda_client)("rba", 30420, type="image")
    t2 = delayed(_forked_uda_client)("/XSX/TCAM/1", 30420)
    t3 = delayed(_forked_uda_client)("/XSX/TCAM/2", 30420)
    t4 = delayed(_forked_uda_client)("/XSX/TCAM/3", 30420)

    dask.compute(t1, t2, t3, t4, schedular=get_sync)


@pytest.mark.skipif(not uda_available, reason="UDA is not available")
def test_load_uda_plugin():
    t1 = LoadUDA("rba", UDAFormat.IDA, type=UDAType.IMAGE)(30420)
    t2 = LoadUDA("/XSX/TCAM/1", UDAFormat.IDA)(30420)
    t3 = LoadUDA("/XSX/TCAM/2", UDAFormat.IDA)(30420)
    t4 = LoadUDA("/XSX/TCAM/3", UDAFormat.IDA)(30420)

    dask.compute(t1, t2, t3, t4)
