from .utils import make_dir_download_file


def worldpop1km(country_code: str) -> None:
    """ Download worldpop1km for country. Note no transformation needed, so downloads
     straight to data/out.
     """

    country_code_upper = country_code.upper()
    foldername = f"data/out/country_extractions/{country_code}/223_popu"

    filename = f"{country_code}_popu_pop_ras_s1_worldpop_pp_popdensity_2020unad.tif"
    url = f"https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km_UNadj/2020/{country_code_upper}/{country_code}_pd_2020_1km_UNadj.tif"
    make_dir_download_file(url, foldername, filename)
