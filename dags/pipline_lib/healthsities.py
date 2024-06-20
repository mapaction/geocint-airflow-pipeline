from logging import getLogger, basicConfig
import requests
import argparse 
import shapefile

BASE_URL = "https://healthsites.io"

basicConfig()

logger = getLogger(__name__)
logger.setLevel("DEBUG")


def healthsites(country_name: str, api_key: str, save_location: str):
    """ Main entry point to this module """
    data = get_health_sites(country_name=country_name, api_key=api_key)
    write_healthsites_shapefile(healthsites=data, output_path=save_location)


def get_health_sites(country_name: str, api_key: str) -> list:
    """ Queries healsites.io v3 api, loops through pages to get data """
    logger.debug("Running get_health_sites for %s", country_name)
    page = 1
    results = []
    new_results = True
    while new_results:
        url = f"{BASE_URL}/api/v3/facilities/?api-key={api_key}&page={page}&country={country_name}"
        response = requests.get(url)
        new_results = response.json()
        results.extend(new_results)
        page += 1
    logger.debug("Finished running get_health_sites. Found %s pages and %s results",
                 page, len(results))
    return results


def write_healthsites_shapefile(healthsites: list, output_path: str) -> None:
    """ Writes input data into a shapefile (.shp, .shx and .dbf) """
    with shapefile.Writer(output_path, shapeType=shapefile.POINT) as w:
        # Define shapefile field records
        w.field('amenity', 'C')
        w.field('name', 'C')
        w.field('operator', 'C')
        w.field('opening_hours', 'C')
        w.field('wheelchair', 'C')
        w.field('emergency', 'C')
        w.field('addr_street', 'C')
        w.field('addr_postcode', 'C')
        w.field('addr_city', 'C')
        w.field('uuid', 'C')
        w.field('osm_id', 'N')

        # Write shapefile
        for site in healthsites:
            x, y = site['centroid']['coordinates']
            w.point(x, y)
            w.record(amenity=site['attributes'].get("amenity", None),
                     name=site['attributes'].get('name', None),
                     operator=site['attributes'].get('operator', None),
                     opening_hours=site['attributes'].get('opening_hours', None),
                     wheelchair=site['attributes'].get('wheelchair', None),
                     emergency=site['attributes'].get('emergency', None),
                     addr_street=site['attributes'].get('addr_street', None),
                     addr_postcode=site['attributes'].get('addr_postcode', None),
                     addr_city=site['attributes'].get('addr_city', None),
                     uuid=site['attributes'].get('uuid', None),
                     osm_id=site['osm_id'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download health sites from healthsites.io")
    parser.add_argument("--country_name", help="Country name", required=True)   
    parser.add_argument("--api_key", help="Healthsites api key", required=True)     
    args = parser.parse_args()

    healthsites(args.country_name, args.api_key, f"{args.country_name}.shp")