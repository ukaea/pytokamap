import pytokamap


def main():
    mapper = pytokamap.load_mapping("zarr.jinja", "globals.json")

    # Datasets are lazily loaded.
    datasets = mapper.load("30420.zarr")
    plasma_current = datasets['amc/plasma_current']
    print(plasma_current)
    
    # Only now is the dataset computed
    plasma_current.compute()
    print(plasma_current)
    
    # Map to a netcdf file
    mapper.to_netcdf("30420.zarr", "30420.nc")


if __name__ == "__main__":
    main()
