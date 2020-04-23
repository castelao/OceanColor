"""Main module."""

import logging
import os
from typing import Any, Dict, Optional, Sequence

import numpy as np
import pandas as pd


module_logger = logging.getLogger("OceanColor.catalog")


def ds_attrs(ds):
    attributes = [
        "product_name",
        "instrument",
        "platform",
        "date_created",
        "time_coverage_start",
        "time_coverage_end",
        "geospatial_lat_min",
        "geospatial_lat_max",
        "geospatial_lon_min",
        "geospatial_lon_max",
    ]
    output = {}
    for a in attributes:
        output[a] = ds.attrs[a]

    for a in ("date_created", "time_coverage_start", "time_coverage_end"):
        assert output[a][-1] == "Z"
        output[a] = np.datetime64(output[a][:-1])

    return output


class Catalog(object):
    """

    ToDo
    ----
    - Register in the store the current database version. This will allow
      migrations in the future.
    - Probably split by sensor and maybe by data type (L2, L3m, ...) as well
      so that one group for each combination, i.e. instead of genreal catalog
      A-L2, A-L3m, ...

    """

    def __init__(self, dbfilename):
        self.store = pd.HDFStore(dbfilename, mode="a", complevel=9, fletcher32=True)

    def __getitem__(self, product_name):
        record = self.store.select("catalog", "index == '{}'".format(product_name))
        if record.size == 0:
            raise KeyError

        return record

    def __contains__(self, product_name):
        try:
            return self[product_name].size > 0
        except KeyError:
            # assert 'catalog' not in self.store
            return False

        ans = ("catalog" in self.store) and self.store.select(
            "catalog",
            where="product_name='%s'" % os.path.basename(product_name),
            columns=["product_name"],
        ).size > 0
        return ans

    def __setitem__(self, key, value):
        self.store.append("catalog", value, format="t", data_columns=True)

    def __del__(self):
        module_logger.debug("Closing Catalog's storage: {}".format(self.store.filename))
        # self.store.flush()
        self.store.close()

    def record(self, ds):
        """
        min_itemsize={'values': 42}
        """
        attrs = ds_attrs(ds)
        assert attrs["product_name"] not in self, (
            "There is a record in the database for %s" % attrs["filename"]
        )
        module_logger.debug("New record: {}".format(attrs))
        attrs = pd.DataFrame([attrs])
        attrs = attrs.set_index("product_name")
        # if ('catalog' in self.store):
        #     tmp = tmp.set_index(tmp.index + self.store.catalog.index.max() + 1)
        self.store.append(
            "catalog", attrs, format="t", data_columns=True, min_itemsize={"values": 42}
        )

    def bloom_filter(
        self,
        track: Sequence[Dict],
        sensor: Optional[Any] = None,
        dtype: Optional[Any] = None,
        dt_tol: Optional[Any] = None,
        dL_tol: Optional[Any] = None,
    ):
        """

        bloom_filter(track, dt_tol, dL_tol)
        """

        cond = []
        cond.append("time_coverage_end >= %r" % (track.time.min() - dt_tol))
        cond.append("time_coverage_start <= %r" % (track.time.max() + dt_tol))
        cond.append("geospatial_lat_max > {}".format(track.lat.min()))
        cond.append("geospatial_lat_min > {}".format(track.lat.max()))
        cond.append(
            "(geospatial_lon_min <= {} & geospatial_lon_max >= {}) or (geospatial_lon_max < 0 & geospatial_lon_min > 0)".format(
                track.lon.max(), track.lon.min()
            )
        )
        for f in self.store.select("catalog", where=cond).index:
            yield f
