import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../main/python'))

import numpy as np
from FindCrop.xmp_utils import transform_corners_to_deskewed, snap_bounds_to_known_size, build_xmp


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


def test_snap_produces_exact_dimensions():
    """Output dimensions must equal the requested exact size regardless of input."""
    top, left, bottom, right = snap_bounds_to_known_size(10, 20, 110, 220, 180, 90)
    assert right - left == 180
    assert bottom - top == 90


def test_snap_preserves_centre():
    """The centre of the box must not move by more than 0.5 px."""
    in_cx = (20 + 220) / 2.0
    in_cy = (10 + 110) / 2.0
    top, left, bottom, right = snap_bounds_to_known_size(10, 20, 110, 220, 180, 90)
    out_cx = (left + right) / 2.0
    out_cy = (top + bottom) / 2.0
    assert abs(out_cx - in_cx) <= 0.5
    assert abs(out_cy - in_cy) <= 0.5


def test_snap_fixes_resize_rounding_error():
    """
    Simulate the resize-then-unscale rounding that find_crop introduces.
    At factor=0.1, floor(5000*0.1)/0.1 may differ from 5000 due to float rounding;
    snap must restore the exact dimension.
    """
    factor = 0.1
    exact_w, exact_h = 5000, 6994
    # Simulate what find_crop produces after resize rounding
    approx_w = int(int(exact_w * factor) / factor)  # may differ from exact_w
    approx_h = int(int(exact_h * factor) / factor)
    top, left = 490, 298
    bottom, right = top + approx_h, left + approx_w

    s_top, s_left, s_bottom, s_right = snap_bounds_to_known_size(
        top, left, bottom, right, exact_w, exact_h
    )
    assert s_right - s_left == exact_w
    assert s_bottom - s_top == exact_h
