import math
from dataclasses import dataclass, field

import cv2
import numpy as np

try:
    from .image_utils import rotate_image, largest_rotated_rect, crop_around_center, rotate_point, resize_image
except ImportError:
    from image_utils import rotate_image, largest_rotated_rect, crop_around_center, rotate_point, resize_image


@dataclass
class FoundCrop:
    corners: np.ndarray
    rotation: float
    score: float
    used_hough: bool = False


def _estimate_skew_hough(
    image: np.ndarray,
    min_lines: int = 10,
    max_std_deg: float = 0.8,
) -> float | None:
    """
    Estimate document skew from near-horizontal Hough lines.

    Runs at a fixed 0.2 scale with 0.1° angular resolution regardless of
    resize_factor, because Hough needs sufficient image detail to find lines
    and is decoupled from the template-matching resize.

    Returns the estimated skew angle in degrees, or None if the estimate is
    not confident (too few lines or high spread).
    """
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY) if image.ndim == 3 else image
    h, w = gray.shape[:2]
    small = cv2.resize(gray, (int(w * 0.2), int(h * 0.2)))
    edges = cv2.Canny(small, 50, 150, apertureSize=3)

    threshold = max(10, int(small.shape[1] * 0.25))
    lines = cv2.HoughLines(edges, 1, np.radians(0.1), threshold=threshold)
    if lines is None:
        return None

    angles = []
    for line in lines:
        deg = np.degrees(line[0][1]) - 90
        if abs(deg) < 10:
            angles.append(deg)

    if len(angles) < min_lines:
        return None
    if np.std(angles) > max_std_deg:
        return None

    return float(np.median(angles))


def _match_at_rotation(big, small, rot, bw, bh, sh, sw, check_inverted):
    """Return (score, corners_in_big_space) for one rotation angle, or None on error."""
    big_rotated = rotate_image(big, rot)
    x_slice, y_slice = crop_around_center(
        big_rotated,
        *largest_rotated_rect(bw, bh, math.radians(rot))
    )
    big_rotated_cropped = big_rotated[y_slice, x_slice]
    ch, cw = big_rotated_cropped.shape[0:2]

    try:
        res = cv2.matchTemplate(big_rotated_cropped, small, cv2.TM_CCOEFF_NORMED)
    except cv2.error:
        return None

    if check_inverted:
        try:
            res2 = cv2.matchTemplate(big_rotated_cropped, cv2.bitwise_not(small), cv2.TM_CCOEFF_NORMED)
            if res2.max() > res.max():
                res = res2
        except cv2.error:
            pass

    rm = res.max()
    loc = np.where(res == rm)
    for pt in zip(*loc[::-1]):
        x0, y0 = pt[0], pt[1]
        x1, y1 = x0 + sw, y0
        x2, y2 = x0 + sw, y0 + sh
        x3, y3 = x0,      y0 + sh

        ox, oy = cw / 2, ch / 2
        x0, y0 = rotate_point(x0, y0, ox, oy, -rot)
        x1, y1 = rotate_point(x1, y1, ox, oy, -rot)
        x2, y2 = rotate_point(x2, y2, ox, oy, -rot)
        x3, y3 = rotate_point(x3, y3, ox, oy, -rot)

        dx, dy = (bw - cw) / 2, (bh - ch) / 2
        points = np.array((
            (x0 + dx, y0 + dy),
            (x1 + dx, y1 + dy),
            (x2 + dx, y2 + dy),
            (x3 + dx, y3 + dy),
        ))
        if np.any(points < 0):
            continue
        if np.any(points[:, 0] > bw) or np.any(points[:, 1] > bh):
            continue

        return rm, points

    return None


def _interpolate_rotation(rotations, scores, best_idx):
    """
    Fit a parabola through the best score and its two neighbours to find the
    sub-grid rotation that would produce the maximum score.  Falls back to the
    grid winner when the neighbours are unavailable or the parabola is flat.
    """
    if best_idx == 0 or best_idx == len(rotations) - 1:
        return rotations[best_idx]

    r0, r1, r2 = rotations[best_idx - 1], rotations[best_idx], rotations[best_idx + 1]
    s0, s1, s2 = scores[best_idx - 1], scores[best_idx], scores[best_idx + 1]
    denom = s0 - 2 * s1 + s2
    if abs(denom) < 1e-10:
        return r1
    peak = r1 - 0.5 * (r2 - r0) * (s2 - s0) / (2 * denom)
    lo, hi = min(r0, r2), max(r0, r2)
    return float(np.clip(peak, lo, hi))


def _run_rotation_search(big, small, bw, bh, sh, sw, check_inverted,
                         rotations, refine_rotation, refine_steps):
    """
    Run a full rotation search over the given angles, with optional two-phase
    refinement.  Returns the best FoundCrop found (used_hough left at default False).
    """
    default_corners = np.array(((0, 0), (bw, 0), (bw, bh), (0, bh)))
    best = FoundCrop(corners=default_corners, rotation=0, score=0)

    scores  = []
    for rot in rotations:
        match = _match_at_rotation(big, small, rot, bw, bh, sh, sw, check_inverted)
        if match is None:
            scores.append(0.0)
            continue
        rm, points = match
        scores.append(rm)
        if rm > best.score:
            best = FoundCrop(corners=points, rotation=rot, score=rm)

    if best.score > 0 and refine_rotation:
        best_idx = int(np.argmax(scores))
        interp_rot = _interpolate_rotation(rotations, scores, best_idx)

        step = abs(rotations[best_idx] - rotations[best_idx - 1]) if best_idx > 0 \
               else abs(rotations[1] - rotations[0])
        fine_rots = np.linspace(interp_rot - step, interp_rot + step, refine_steps + 1)

        for rot in fine_rots:
            if rot == rotations[best_idx]:
                continue
            match = _match_at_rotation(big, small, rot, bw, bh, sh, sw, check_inverted)
            if match is None:
                continue
            rm, points = match
            if rm > best.score:
                best = FoundCrop(corners=points, rotation=rot, score=rm)

    return best


def find_crop(uncropped_image: np.ndarray, cropped_image: np.ndarray,
              resize_factor: float = 1.0, rotations=None,
              check_inverted: bool = True,
              refine_rotation: bool = True,
              refine_steps: int = 20,
              use_hough: bool = True,
              hough_fallback_threshold: float = 0.5) -> FoundCrop:
    big   = resize_image(uncropped_image, factor=resize_factor)
    small = resize_image(cropped_image,   factor=resize_factor)

    bh, bw = big.shape[0:2]
    sh, sw = small.shape[0:2]

    if rotations is None:
        rots = np.linspace(-1, 1, 51)
        coarse_rotations = list(4 * rots ** 3 + rots)
    else:
        coarse_rotations = list(rotations)

    # ── Hough-seeded fast path ────────────────────────────────────────────────
    if use_hough:
        hough_angle = _estimate_skew_hough(uncropped_image)
        if hough_angle is not None:
            seeded_rots = list(np.linspace(hough_angle - 2.0, hough_angle + 2.0, 21))
            result = _run_rotation_search(big, small, bw, bh, sh, sw, check_inverted,
                                          seeded_rots, refine_rotation, refine_steps)
            if result.score >= hough_fallback_threshold:
                result.used_hough = True
                result.corners = (result.corners / resize_factor).astype(int)
                return result

    # ── Full grid fallback ────────────────────────────────────────────────────
    result = _run_rotation_search(big, small, bw, bh, sh, sw, check_inverted,
                                  coarse_rotations, refine_rotation, refine_steps)
    result.corners = (result.corners / resize_factor).astype(int)
    return result
