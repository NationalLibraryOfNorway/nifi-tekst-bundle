import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../main/python'))

import math
import numpy as np
import cv2

from FindCrop.find_crop import find_crop, _estimate_skew_hough
from FindCrop.image_utils import rotate_image, rotate_point
from FindCrop.xmp_utils import transform_corners_to_deskewed


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_synthetic_pair(rotation_deg=2.0, resize_factor=1.0):
    """
    Return (uncropped, cropped, expected_crop_bounds) where:
    - uncropped is a 400x600 image with a checkerboard region at rows 100-300, cols 150-450.
    - uncropped is then rotated by rotation_deg (so find_crop must recover that angle).
    - cropped is the interior of that rotated region (inset 30px on each side).
    - expected_crop_bounds is (top, left, bottom, right) of the interior in the *original* image.
    """
    rng = np.random.default_rng(0)
    uncropped = rng.integers(20, 180, (400, 600, 3), dtype=np.uint8)

    top, left, bottom, right = 100, 150, 300, 450
    for r in range(top, bottom):
        for c in range(left, right):
            uncropped[r, c] = [255, 0, 0] if ((r + c) % 16 < 8) else [0, 200, 0]

    h, w = uncropped.shape[:2]
    M = cv2.getRotationMatrix2D((w / 2, h / 2), rotation_deg, 1.0)
    rotated_uncropped = cv2.warpAffine(
        uncropped, M, (w, h), flags=cv2.INTER_LINEAR, borderMode=cv2.BORDER_REPLICATE
    )

    inset = 30
    cropped = rotated_uncropped[top + inset:bottom - inset, left + inset:right - inset].copy()
    return uncropped, cropped, (top + inset, left + inset, bottom - inset, right - inset)


# ---------------------------------------------------------------------------
# rotate_point: un-rotation must be the inverse of rotate_image
# ---------------------------------------------------------------------------

def test_rotate_point_unrotates_correctly():
    """
    rotate_point with -angle must produce substantially less error than +angle
    when un-rotating a point that was moved by rotate_image with +angle.

    This is the regression test for the sign bug: find_crop called
    rotate_point(x, y, ox, oy, +rot) to back-project template corners, but the
    correct inverse of rotate_image(img, +rot) is rotate_point(..., -rot).
    """
    img = np.zeros((400, 600, 3), dtype=np.uint8)
    orig_x, orig_y = 500, 80
    cv2.rectangle(img, (orig_x - 5, orig_y - 5), (orig_x + 5, orig_y + 5), (255, 255, 255), -1)

    angle = 3.0
    rotated = rotate_image(img, angle)
    rh, rw = rotated.shape[:2]

    locs = np.where(rotated[:, :, 0] > 200)
    assert len(locs[0]) > 0, "Pixel not found after rotation"
    rx = int(np.mean(locs[1]))
    ry = int(np.mean(locs[0]))

    ox, oy = rw / 2, rh / 2

    back_x_neg, back_y_neg = rotate_point(rx, ry, ox, oy, -angle)
    back_x_pos, back_y_pos = rotate_point(rx, ry, ox, oy, +angle)

    err_neg = math.hypot(back_x_neg - orig_x, back_y_neg - orig_y)
    err_pos = math.hypot(back_x_pos - orig_x, back_y_pos - orig_y)

    assert err_neg < err_pos, (
        f"rotate_point(-angle) error ({err_neg:.1f}px) should be less than "
        f"rotate_point(+angle) error ({err_pos:.1f}px): -angle is the correct inverse"
    )
    # Also require that -angle is reasonably close (canvas resize adds small translation)
    assert err_neg < 10, (
        f"rotate_point(-angle) error {err_neg:.1f}px is too large "
        f"(got ({back_x_neg},{back_y_neg}), expected ~({orig_x},{orig_y}))"
    )


def test_rotate_point_wrong_sign_produces_large_error():
    """Confirm that passing +angle (the old bug) gives a significantly larger error."""
    img = np.zeros((400, 600, 3), dtype=np.uint8)
    orig_x, orig_y = 500, 80
    cv2.rectangle(img, (orig_x - 5, orig_y - 5), (orig_x + 5, orig_y + 5), (255, 255, 255), -1)

    angle = 3.0
    rotated = rotate_image(img, angle)
    rh, rw = rotated.shape[:2]

    locs = np.where(rotated[:, :, 0] > 200)
    rx = int(np.mean(locs[1]))
    ry = int(np.mean(locs[0]))

    ox, oy = rw / 2, rh / 2

    back_x_wrong, back_y_wrong = rotate_point(rx, ry, ox, oy, +angle)
    err_wrong = math.hypot(back_x_wrong - orig_x, back_y_wrong - orig_y)
    # At 3° on an off-centre pixel the wrong sign causes >10 px error
    assert err_wrong > 10, (
        f"Expected +angle to produce large error (>10px) to document the bug, got {err_wrong:.1f}px"
    )


# ---------------------------------------------------------------------------
# find_crop: integration round-trip with a known rotation
# ---------------------------------------------------------------------------

def test_find_crop_recovers_known_rotation():
    """find_crop must detect the applied rotation within ±0.1° with refinement enabled."""
    rotation = 2.0
    uncropped, cropped, _ = make_synthetic_pair(rotation_deg=rotation)
    result = find_crop(uncropped, cropped, resize_factor=0.3, check_inverted=False,
                       refine_rotation=True)
    assert abs(result.rotation - rotation) < 0.1, (
        f"Expected rotation ~{rotation}°, got {result.rotation:.4f}°"
    )


def test_refinement_improves_rotation_accuracy():
    """refine_rotation=True must give meaningfully lower error than refine_rotation=False."""
    rotation = 3.5  # large angle where the coarse grid is sparsest
    uncropped, cropped, _ = make_synthetic_pair(rotation_deg=rotation)
    coarse = find_crop(uncropped, cropped, resize_factor=0.3, check_inverted=False,
                       refine_rotation=False)
    refined = find_crop(uncropped, cropped, resize_factor=0.3, check_inverted=False,
                        refine_rotation=True)
    err_coarse  = abs(coarse.rotation  - rotation)
    err_refined = abs(refined.rotation - rotation)
    assert err_refined < err_coarse, (
        f"Refinement should reduce error: coarse={err_coarse:.4f}° refined={err_refined:.4f}°"
    )
    assert err_refined < 0.05, (
        f"Refined error {err_refined:.4f}° should be under 0.05°"
    )


def test_find_crop_score_above_threshold():
    """A clearly matching crop must produce a score > 0.8."""
    uncropped, cropped, _ = make_synthetic_pair(rotation_deg=1.5)
    result = find_crop(uncropped, cropped, resize_factor=0.3, check_inverted=False)
    assert result.score > 0.8, f"Expected score > 0.8, got {result.score:.4f}"


def test_find_crop_corners_in_original_image_bounds():
    """Returned corners (after resize scaling) must lie within the original image."""
    uncropped, cropped, _ = make_synthetic_pair(rotation_deg=2.0)
    h, w = uncropped.shape[:2]
    result = find_crop(uncropped, cropped, resize_factor=0.3, check_inverted=False)
    pts = result.corners
    assert np.all(pts >= 0), f"Some corners are negative: {pts}"
    assert np.all(pts[:, 0] <= w), f"Some x-corners exceed image width {w}: {pts}"
    assert np.all(pts[:, 1] <= h), f"Some y-corners exceed image height {h}: {pts}"


def test_find_crop_matching_score_higher_than_mismatched():
    """
    A correct (matching) crop must score higher than a crop taken from a
    completely different image — verifying that the score is meaningful.
    """
    rng = np.random.default_rng(7)
    uncropped_a = rng.integers(20, 180, (400, 600, 3), dtype=np.uint8)
    uncropped_b = rng.integers(20, 180, (400, 600, 3), dtype=np.uint8)

    # Paste the same distinctive checkerboard on both
    for r in range(100, 300):
        for c in range(150, 450):
            uncropped_a[r, c] = [255, 0, 0] if ((r + c) % 16 < 8) else [0, 200, 0]
            uncropped_b[r, c] = [0, 0, 255] if ((r + c) % 16 < 8) else [200, 200, 0]

    # True crop from A (no rotation for simplicity)
    true_crop = uncropped_a[120:280, 170:430].copy()
    # False crop from B — same region but different colours
    false_crop = uncropped_b[120:280, 170:430].copy()

    score_match = find_crop(uncropped_a, true_crop, resize_factor=0.5, check_inverted=False).score
    score_mismatch = find_crop(uncropped_a, false_crop, resize_factor=0.5, check_inverted=False).score

    assert score_match > score_mismatch, (
        f"Matching crop score ({score_match:.4f}) should exceed mismatching score ({score_mismatch:.4f})"
    )


# ---------------------------------------------------------------------------
# end-to-end: find_crop + transform_corners_to_deskewed corner accuracy
# ---------------------------------------------------------------------------

def test_end_to_end_crop_bounds_accuracy():
    """
    After applying the correct -rotation un-rotation in rotate_point,
    the recovered crop bounds must be within 40px of the expected bounds.
    """
    rotation = 2.0
    uncropped, cropped, (exp_top, exp_left, exp_bottom, exp_right) = make_synthetic_pair(
        rotation_deg=rotation
    )
    result = find_crop(uncropped, cropped, resize_factor=0.3, check_inverted=False)

    img_h, img_w = uncropped.shape[:2]
    top, left, bottom, right = transform_corners_to_deskewed(
        result.corners, result.rotation, img_w, img_h
    )

    tol = 40
    assert abs(top - exp_top) < tol, f"top off by {abs(top - exp_top)}px (got {top}, expected ~{exp_top})"
    assert abs(left - exp_left) < tol, f"left off by {abs(left - exp_left)}px (got {left}, expected ~{exp_left})"
    assert abs(bottom - exp_bottom) < tol, f"bottom off by {abs(bottom - exp_bottom)}px"
    assert abs(right - exp_right) < tol, f"right off by {abs(right - exp_right)}px"


# ---------------------------------------------------------------------------
# Hough estimation
# ---------------------------------------------------------------------------

def test_hough_returns_none_on_featureless_image():
    """A uniform image has no detectable lines — Hough must return None."""
    blank = np.full((400, 600, 3), 128, dtype=np.uint8)
    assert _estimate_skew_hough(blank) is None


def test_hough_detects_near_horizontal_skew():
    """An image with strong horizontal lines should return an angle near 0°."""
    img = np.zeros((1000, 1200, 3), dtype=np.uint8)
    for y in range(80, 1000, 60):
        cv2.line(img, (0, y), (1200, y), (255, 255, 255), 5)
    angle = _estimate_skew_hough(img)
    assert angle is not None, 'Expected a confident Hough estimate on a ruled image'
    assert abs(angle) < 1.0, f'Expected near 0°, got {angle:.3f}°'


def test_find_crop_sets_used_hough_false_when_disabled():
    """used_hough must be False when use_hough=False."""
    uncropped, cropped, _ = make_synthetic_pair(rotation_deg=2.0)
    result = find_crop(uncropped, cropped, resize_factor=0.3,
                       check_inverted=False, use_hough=False)
    assert result.used_hough is False


def test_find_crop_used_hough_attribute_is_bool():
    """used_hough on FoundCrop is always a bool."""
    uncropped, cropped, _ = make_synthetic_pair(rotation_deg=2.0)
    result = find_crop(uncropped, cropped, resize_factor=0.3, check_inverted=False)
    assert isinstance(result.used_hough, bool)
