import xml.etree.ElementTree as ET

# NB access images are scanned at 400 DPI.
# 1 mm10 (tenth of a mm) = 400 / (25.4 * 10) pixels at that resolution.
NB_ACCESS_SCALE: float = 400 / (25.4 * 10)


def parse_alto_page_dimensions(alto_xml: str) -> tuple[int, int]:
    """Return (width_mm10, height_mm10) from the first Page element in an ALTO XML string."""
    root = ET.fromstring(alto_xml)
    page = root.find('.//Page')
    if page is None:
        raise ValueError('No Page element found in ALTO XML')
    return int(page.get('WIDTH')), int(page.get('HEIGHT'))


def alto_to_pixels(mm10: float, scale: float = NB_ACCESS_SCALE) -> int:
    """Convert an ALTO mm10 measurement to pixels using the given px-per-mm10 scale."""
    return round(mm10 * scale)
