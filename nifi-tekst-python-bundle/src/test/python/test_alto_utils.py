import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../main/python'))

from FindCrop.alto_utils import parse_alto_page_dimensions, alto_to_pixels

_ALTO_TEMPLATE = """\
<?xml version="1.0" encoding="UTF-8"?>
<alto xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <Description>
    <MeasurementUnit>mm10</MeasurementUnit>
  </Description>
  <Layout>
    <Page ID="P1" HEIGHT="{h}" WIDTH="{w}" PC="0.95"/>
  </Layout>
</alto>
"""


def _make_alto(w, h):
    return _ALTO_TEMPLATE.format(w=w, h=h)


def test_parse_returns_width_and_height():
    w, h = parse_alto_page_dimensions(_make_alto(3175, 4441))
    assert w == 3175
    assert h == 4441


def test_parse_returns_integers():
    w, h = parse_alto_page_dimensions(_make_alto(1000, 2000))
    assert isinstance(w, int)
    assert isinstance(h, int)


def test_parse_raises_on_missing_page():
    xml = '<?xml version="1.0"?><alto><Layout></Layout></alto>'
    with pytest.raises(ValueError, match='No Page element'):
        parse_alto_page_dimensions(xml)


def test_parse_real_alto_file():
    """Smoke test against the actual access ALTO in test resources."""
    resources = os.path.join(os.path.dirname(__file__), '..', 'resources')
    alto_path = os.path.join(resources, 'cropped-alto.xml')
    with open(alto_path, 'r', encoding='utf-8') as f:
        xml = f.read()
    w, h = parse_alto_page_dimensions(xml)
    assert w == 3175
    assert h == 4441


def test_alto_to_pixels_at_400dpi():
    # 400 DPI = 400 / (25.4 * 10) px per mm10 = 1.5748...
    scale = 400 / (25.4 * 10)
    assert alto_to_pixels(3175, scale) == 5000
    assert alto_to_pixels(4441, scale) == 6994  # round(6993.7) = 6994


def test_alto_to_pixels_returns_int():
    scale = 1.5748
    result = alto_to_pixels(1000, scale)
    assert isinstance(result, int)


def test_alto_scale_derived_from_known_image():
    """Scale derived from cropped image dims matches expected pixel conversion."""
    alto_w_mm10, alto_h_mm10 = 3175, 4441
    img_w_px, img_h_px = 5000, 6994

    scale = img_w_px / alto_w_mm10
    assert abs(scale - 1.5748) < 0.001

    assert alto_to_pixels(alto_w_mm10, scale) == img_w_px
    assert abs(alto_to_pixels(alto_h_mm10, scale) - img_h_px) <= 1
