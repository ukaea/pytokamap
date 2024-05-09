import pytokamap

def main():
    mapper = pytokamap.load_mapping("uda.jinja", "globals.json")
    mapper.to_zarr(30420, "30420.zarr")

if __name__ == "__main__":
    main()