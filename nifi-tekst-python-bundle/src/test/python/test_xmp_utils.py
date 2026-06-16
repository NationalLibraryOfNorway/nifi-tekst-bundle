import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../main/python'))

import numpy as np
from FindCrop.xmp_utils import transform_corners_to_deskewed, build_xmp


def test_no_rotation_returns_exact_bounds():
    # Axis-aligned rectangle at (100,150)→(300,250) in a 600×400 image.
    # Zero rotation means deskewed bounds == original bounds.
    corners = np.array([[100, 150], [300, 150], [300, 250], [100, 250]])
    top, left, bottom, right = transform_corners_to_deskewed(corners, 0.0, 600, 400)
    assert left   == 100
    assert top    == 150
    assert right  == 300
    assert bottom == 250


def test_returns_integers():
    corners = np.array([[100, 100], [300, 100], [300, 200], [100, 200]])
    top, left, bottom, right = transform_corners_to_deskewed(corners, 0.0, 600, 400)
    for v in (top, left, bottom, right):
        assert isinstance(v, (int, np.integer)), f"Expected int, got {type(v)}"


def test_rotation_expands_bounding_box():
    # A rectangle centred on the image. Rotating 45° should widen its bounding box.
    corners = np.array([[250, 150], [350, 150], [350, 250], [250, 250]])
    _, left0, _, right0 = transform_corners_to_deskewed(corners, 0.0, 600, 400)
    _, left45, _, right45 = transform_corners_to_deskewed(corners, 45.0, 600, 400)
    assert (right45 - left45) > (right0 - left0)


def test_build_xmp_contains_all_crs_fields():
    xmp = build_xmp(2.3, 150, 100, 250, 300)
    assert 'crs:HasCrop="True"'   in xmp
    assert 'crs:CropAngle="2.3"' in xmp
    assert 'crs:CropTop="150"'   in xmp
    assert 'crs:CropLeft="100"'  in xmp
    assert 'crs:CropBottom="250"' in xmp
    assert 'crs:CropRight="300"' in xmp
    assert 'crs:CropUnit="3"'    in xmp


def test_build_xmp_is_valid_xml():
    from xml.etree import ElementTree
    xmp = build_xmp(0.0, 0, 0, 100, 100)
    ElementTree.fromstring(xmp)  # raises on malformed XML


def test_build_xmp_uses_crs_namespace():
    xmp = build_xmp(0.0, 0, 0, 100, 100)
    assert 'http://ns.adobe.com/camera-raw-settings/1.0/' in xmp
