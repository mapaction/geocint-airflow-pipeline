from .utils import make_dir_download_file


def worldpop100m(country_code: str) -> None:
    """ Download worldpop100m for country. Note no transformation needed, so downloads
     straight to data/out.

     """
    country_code_upper = country_code.upper()
    # foldername = f"data/input/{country_code}/223_popu"

    foldername = f"data/out/country_extractions/{country_code}/223_popu"
    # filename = f"{country_code}_popu_pop_ras_s1_worldpop_pp_popdensity_2020unad.tif"

    filename = f"{country_code}_popu_pop_ras_s1_worldpop_pp_2020unadj_100m.tif"

    url = f"https://data.worldpop.org/GIS/Population/Global_2000_2020/2020/{country_code_upper}/{country_code}_ppp_2020_UNadj.tif"
    make_dir_download_file(url, foldername, filename)
