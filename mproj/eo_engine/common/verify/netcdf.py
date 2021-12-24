from pathlib import Path


def check_netcdf(filepath: Path) -> bool:
    try:
        import xarray as xr
        dataset = xr.open_dataset(filepath)
        dataset.load()
    except Exception:
        return False
