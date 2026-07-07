"""
End-to-end processor tests.

nifiapi is only available inside a running NiFi instance, so we inject a
lightweight stub via sys.modules before importing FindCrop.py.  The stub
mirrors only the surface the processor actually calls.
"""

import sys
import os
import tempfile
import types
import xml.etree.ElementTree as ET

import numpy as np
import cv2

# ── nifiapi stub ─────────────────────────────────────────────────────────────

class _FlowFileTransform:
    def __init__(self, **kwargs):
        pass

class _FlowFileTransformResult:
    def __init__(self, relationship, contents=None, attributes=None):
        self.relationship = relationship
        self.contents = contents
        self.attributes = attributes or {}

class _PropertyDescriptor:
    def __init__(self, name='', **kwargs):
        self.name = name

class _StandardValidators:
    NUMBER_VALIDATOR = None

class _ExpressionLanguageScope:
    FLOWFILE_ATTRIBUTES = None

_mod_fft = types.ModuleType('nifiapi.flowfiletransform')
_mod_fft.FlowFileTransform = _FlowFileTransform
_mod_fft.FlowFileTransformResult = _FlowFileTransformResult

_mod_props = types.ModuleType('nifiapi.properties')
_mod_props.PropertyDescriptor = _PropertyDescriptor
_mod_props.StandardValidators = _StandardValidators
_mod_props.ExpressionLanguageScope = _ExpressionLanguageScope

sys.modules.setdefault('nifiapi', types.ModuleType('nifiapi'))
sys.modules.setdefault('nifiapi.flowfiletransform', _mod_fft)
sys.modules.setdefault('nifiapi.properties', _mod_props)

# ── import processor (nifiapi stub is now in place) ──────────────────────────

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../main/python'))

from FindCrop.FindCrop import FindCrop  # noqa: E402

# ── test doubles ─────────────────────────────────────────────────────────────

class _PropertyValue:
    def __init__(self, value):
        self._v = value

    def getValue(self):
        return self._v

    def evaluateAttributeExpressions(self, flowfile):
        return self


class _MockContext:
    def __init__(self, props: dict):
        self._props = props

    def getProperty(self, name):
        return _PropertyValue(self._props.get(name))


class _MockFlowFile:
    def getAttribute(self, name):
        return None


class _MockLogger:
    def error(self, msg): pass
    def info(self, msg): pass
    def warn(self, msg): pass


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_synthetic_pair(rotation_deg=2.0):
    """400×600 image with a checkerboard region rotated by rotation_deg."""
    rng = np.random.default_rng(0)
    uncropped = rng.integers(20, 180, (400, 600, 3), dtype=np.uint8)

    t, l, b, r = 100, 150, 300, 450
    for row in range(t, b):
        for col in range(l, r):
            uncropped[row, col] = [255, 0, 0] if ((row + col) % 16 < 8) else [0, 200, 0]

    h, w = uncropped.shape[:2]
    M = cv2.getRotationMatrix2D((w / 2, h / 2), rotation_deg, 1.0)
    rotated = cv2.warpAffine(uncropped, M, (w, h), flags=cv2.INTER_LINEAR, borderMode=cv2.BORDER_REPLICATE)

    inset = 30
    cropped = rotated[t + inset:b - inset, l + inset:r - inset].copy()
    return uncropped, cropped


def _write_pair(tmpdir, uncropped, cropped):
    up = os.path.join(tmpdir, 'uncropped.png')
    cp = os.path.join(tmpdir, 'cropped.png')
    cv2.imwrite(up, uncropped)
    cv2.imwrite(cp, cropped)
    return up, cp


def _make_processor():
    p = FindCrop()
    p.logger = _MockLogger()
    return p


def _make_context(uncropped_path, cropped_path, resize_factor='0.3', check_inverted='false',
                  alto_path=None, tolerance='200'):
    props = {
        'Uncropped Image Path': uncropped_path,
        'Cropped Image Path': cropped_path,
        'Resize Factor': resize_factor,
        'Check Inverted': check_inverted,
        'Dimension Tolerance': tolerance,
        'Use Hough Estimation': 'true',
    }
    if alto_path is not None:
        props['Alto XML Path'] = alto_path
    return _MockContext(props)


# ── tests ────────────────────────────────────────────────────────────────────

def test_processor_routes_to_success_on_matching_images():
    with tempfile.TemporaryDirectory() as tmpdir:
        unc, crop = _make_synthetic_pair(rotation_deg=2.0)
        up, cp = _write_pair(tmpdir, unc, crop)

        result = _make_processor().transform(_make_context(up, cp), _MockFlowFile())

    assert result.relationship == 'success'


def test_processor_always_emits_used_hough_attribute():
    with tempfile.TemporaryDirectory() as tmpdir:
        unc, crop = _make_synthetic_pair(rotation_deg=2.0)
        up, cp = _write_pair(tmpdir, unc, crop)

        result = _make_processor().transform(_make_context(up, cp), _MockFlowFile())

    assert result.relationship == 'success'
    assert result.attributes.get('crop.used_hough') in ('true', 'false')


def test_processor_output_is_valid_xmp_with_crs_namespace():
    with tempfile.TemporaryDirectory() as tmpdir:
        unc, crop = _make_synthetic_pair(rotation_deg=1.5)
        up, cp = _write_pair(tmpdir, unc, crop)

        result = _make_processor().transform(_make_context(up, cp), _MockFlowFile())

    assert result.relationship == 'success'
    xmp = result.contents.decode('utf-8')
    ET.fromstring(xmp)  # raises if not valid XML
    assert 'crs:HasCrop="True"' in xmp
    assert 'crs:CropAngle' in xmp
    assert 'crs:CropUnit="3"' in xmp
    assert 'http://ns.adobe.com/camera-raw-settings/1.0/' in xmp


def test_processor_crop_score_attribute_is_between_0_and_1():
    with tempfile.TemporaryDirectory() as tmpdir:
        unc, crop = _make_synthetic_pair(rotation_deg=2.0)
        up, cp = _write_pair(tmpdir, unc, crop)

        result = _make_processor().transform(_make_context(up, cp), _MockFlowFile())

    assert result.relationship == 'success'
    score = float(result.attributes['crop.score'])
    assert 0.0 < score <= 1.0, f"Unexpected score: {score}"


def test_processor_crop_rotation_attribute_matches_applied_rotation():
    rotation = 2.0
    with tempfile.TemporaryDirectory() as tmpdir:
        unc, crop = _make_synthetic_pair(rotation_deg=rotation)
        up, cp = _write_pair(tmpdir, unc, crop)

        result = _make_processor().transform(_make_context(up, cp), _MockFlowFile())

    assert result.relationship == 'success'
    detected = float(result.attributes['crop.rotation'])
    assert abs(detected - rotation) < 1.0, f"Expected ~{rotation}°, got {detected:.3f}°"


def test_processor_xmp_crop_bounds_are_plausible():
    """CropTop/Left/Bottom/Right must be positive integers within image dimensions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        unc, crop = _make_synthetic_pair(rotation_deg=2.0)
        up, cp = _write_pair(tmpdir, unc, crop)
        img_h, img_w = unc.shape[:2]

        result = _make_processor().transform(_make_context(up, cp), _MockFlowFile())

    assert result.relationship == 'success'
    xmp = result.contents.decode('utf-8')

    import re
    def extract(field):
        m = re.search(rf'crs:{field}="([^"]+)"', xmp)
        assert m, f"crs:{field} not found in XMP"
        return int(m.group(1))

    top    = extract('CropTop')
    left   = extract('CropLeft')
    bottom = extract('CropBottom')
    right  = extract('CropRight')

    assert 0 <= top < bottom <= img_h, f"top={top} bottom={bottom} img_h={img_h}"
    assert 0 <= left < right <= img_w, f"left={left} right={right} img_w={img_w}"


def test_processor_routes_to_failure_when_uncropped_path_missing():
    with tempfile.TemporaryDirectory() as tmpdir:
        _, crop = _make_synthetic_pair()
        _, cp = _write_pair(tmpdir, np.zeros((10, 10, 3), dtype=np.uint8), crop)

        result = _make_processor().transform(
            _make_context('/nonexistent/uncropped.png', cp), _MockFlowFile()
        )

    assert result.relationship == 'failure'


def test_processor_routes_to_failure_when_cropped_path_missing():
    with tempfile.TemporaryDirectory() as tmpdir:
        unc, _ = _make_synthetic_pair()
        up, _ = _write_pair(tmpdir, unc, np.zeros((10, 10, 3), dtype=np.uint8))

        result = _make_processor().transform(
            _make_context(up, '/nonexistent/cropped.png'), _MockFlowFile()
        )

    assert result.relationship == 'failure'


# ── real-file integration tests (JP2 + ALTO) ─────────────────────────────────

import pytest

RESOURCES = os.path.join(os.path.dirname(__file__), '..', 'resources')
_UNCROPPED_JP2 = os.path.join(RESOURCES, 'uncropped.jp2')
_CROPPED_JP2   = os.path.join(RESOURCES, 'cropped.jp2')
_ALTO_XML      = os.path.join(RESOURCES, 'cropped-alto.xml')

_real_files = pytest.mark.skipif(
    not all(os.path.exists(p) for p in (_UNCROPPED_JP2, _CROPPED_JP2, _ALTO_XML)),
    reason='Real JP2/ALTO test files not present in src/test/resources/',
)


@_real_files
def test_real_images_route_to_success():
    """Processor finds the crop in the real JP2 pair and routes to success."""
    result = _make_processor().transform(
        _make_context(_UNCROPPED_JP2, _CROPPED_JP2, resize_factor='0.1'),
        _MockFlowFile(),
    )
    assert result.relationship == 'success'
    assert float(result.attributes['crop.score']) > 0.7


@_real_files
def test_real_images_hough_seeded_and_attribute_set():
    """
    Real JP2 pages have clear text lines — Hough should succeed and
    the processor must report crop.used_hough=true.
    """
    result = _make_processor().transform(
        _make_context(_UNCROPPED_JP2, _CROPPED_JP2, resize_factor='0.1'),
        _MockFlowFile(),
    )
    assert result.relationship == 'success'
    assert result.attributes.get('crop.used_hough') == 'true', (
        'Expected Hough to succeed on a real document page'
    )


@_real_files
def test_real_images_crop_dims_match_actual_cropped_image():
    """
    Detected crop bounds must produce dimensions within 200 px of the actual
    cropped image.  This is the core 'does find_crop work on real scans' test.
    """
    import re
    actual_crop = cv2.imread(_CROPPED_JP2)
    expected_w, expected_h = actual_crop.shape[1], actual_crop.shape[0]

    result = _make_processor().transform(
        _make_context(_UNCROPPED_JP2, _CROPPED_JP2, resize_factor='0.1'),
        _MockFlowFile(),
    )
    assert result.relationship == 'success'

    xmp = result.contents.decode('utf-8')
    def extract(field):
        m = re.search(rf'crs:{field}="([^"]+)"', xmp)
        assert m, f'crs:{field} not in XMP'
        return int(m.group(1))

    top, left, bottom, right = extract('CropTop'), extract('CropLeft'), extract('CropBottom'), extract('CropRight')
    found_w, found_h = right - left, bottom - top

    assert found_w == expected_w, f'Width mismatch: {found_w} != {expected_w}'
    assert found_h == expected_h, f'Height mismatch: {found_h} != {expected_h}'


@_real_files
def test_real_images_with_alto_dimension_match():
    """
    When the access ALTO is provided, the processor must confirm dimension match
    and set crop.dimension_match=true for the real JP2 pair.
    """
    result = _make_processor().transform(
        _make_context(
            _UNCROPPED_JP2, _CROPPED_JP2,
            resize_factor='0.1',
            alto_path=_ALTO_XML,
            tolerance='200',
        ),
        _MockFlowFile(),
    )
    assert result.relationship == 'success', (
        f'Expected success, got {result.relationship}. '
        f'error_w={result.attributes.get("crop.dimension_error_w")}, '
        f'error_h={result.attributes.get("crop.dimension_error_h")}'
    )
    assert result.attributes.get('crop.dimension_match') == 'true', (
        f'dimension_match not true: '
        f'error_w={result.attributes.get("crop.dimension_error_w")}, '
        f'error_h={result.attributes.get("crop.dimension_error_h")}'
    )


@_real_files
def test_wrong_alto_routes_to_dimension_mismatch():
    """
    An ALTO whose page dimensions describe a different physical page must trigger
    dimension_mismatch.  We write a synthetic ALTO with half the correct dimensions.
    """
    wrong_alto = """\
<?xml version="1.0" encoding="UTF-8"?>
<alto>
  <Description><MeasurementUnit>mm10</MeasurementUnit></Description>
  <Layout>
    <Page ID="P1" HEIGHT="2200" WIDTH="1587" PC="0.95"/>
  </Layout>
</alto>
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
        f.write(wrong_alto)
        wrong_path = f.name

    try:
        result = _make_processor().transform(
            _make_context(
                _UNCROPPED_JP2, _CROPPED_JP2,
                resize_factor='0.1',
                alto_path=wrong_path,
                tolerance='200',
            ),
            _MockFlowFile(),
        )
    finally:
        os.unlink(wrong_path)

    assert result.relationship == 'dimension_mismatch'
    assert result.attributes.get('crop.dimension_match') == 'false'
