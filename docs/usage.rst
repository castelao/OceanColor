=====
Usage
=====

Register
--------

The first step is to create an account at `NASA's EarthData <https://urs.earthdata.nasa.gov/users/new>`_  so you can access to NASA's data.

Along this manual, whenever you see <earthdata-username> & <earthdata-password> replace it by your username and password.

Inside Python
-------------

To use Ocean Color in a project::

    import OceanColor

    db = OceanColorDB(<earthdata-username>, <earthdata-password>)
    db.backend = FileSystem('./')

    sensor = 'aqua'
    dtype = 'L2'
    dL_tol = 12e3
    dt_tol = timedelta64(12, 'h')
    track = DataFrame([
        {"time": datetime64("2016-09-01 10:00:00"), "lat": 35.6, "lon": -126.81},
        {"time": datetime64("2016-09-01 22:00:00"), "lat": 34, "lon": -126}])

    matchup = InRange(username, password, './', npes=3)
    matchup.search(track, sensor, dtype, dt_tol, dL_tol)
    for m in matchup:
      print(m)

For more examples, check the collection of notebooks using `Pangeo <https://binder.pangeo.io/v2/gh/castelao/OceanColor/main?filepath=docs%2Fnotebooks%2F>`_

Command line (shell)
--------------------

In a shell terminal, once could run::

    OceanColor InRange \
      --username=<earthdata-username> \
      --password=<earthdata-password> \
      --data-type=L2 \
      --time-tolerance=6 \
      --distance-tolerance=5e3 \
      2019-05-21T12:00:00,15,-38
