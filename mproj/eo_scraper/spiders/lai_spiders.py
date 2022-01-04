from eo_engine.models import EOSourceGroupChoices
from eo_scraper.spiders.abstract_spiders import CopernicusSpider


# special rules
# if this thing starts to bloat up, maybe consider splitting into different
# pipelines


class LAI300mV1Spider(CopernicusSpider):
    name = EOSourceGroupChoices.S02P02_LAI_300M_V1_GLOB_CGLS
    start_urls = [
        f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Properties/LAI_300m_V1/"
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
