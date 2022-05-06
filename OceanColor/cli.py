"""Console script for ocean_color."""
import sys
import click
import numpy as np
import pandas as pd

from OceanColor.inrange import InRange


# At this point it's just a Proof of concept
#  OceanColor InRange --username=myUser --password=myPassword 2019-05-21,15,-38

@click.group()
def main():
    """Console script for OceanColor."""
    pass


@main.command(name="InRange")
@click.option('--username', required=True,
    help="Username on NASA's EarthData System")
@click.option('--password', required=True,
    help="Password on NASA's EarthData System")
@click.option('--sensor', type=click.Choice(['aqua', 'terra']),
        default='aqua')
@click.option('--data-type', 'dtype', type=click.Choice(['L2', 'L3m']),
        default='L3m')
@click.option('--time-tolerance', 'dt_tol', type=click.INT , default=12,
              help='Time difference [hours] tolerance to matchup')
@click.option('--distance-tolerance', 'dL_tol', type=float, default=10e3,
              help='Distance difference [m] tolerance to matchup')
@click.argument('track', required=True)
def cli_inrange(username, password, sensor, dtype, dt_tol, dL_tol, track):
    time, lat, lon = track.split(',')
    track = pd.DataFrame({"time": [np.datetime64(time)],
                          "lat": [float(lat)],
                          "lon": [float(lon)]})

    dt_tol = np.timedelta64(dt_tol, 'h')
    matchup = InRange(username, password, npes=3)
    matchup.search(track, sensor, dtype, dt_tol, dL_tol)
    for m in matchup:
        click.echo(m)

    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
