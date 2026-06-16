import math
from dataclasses import dataclass

import cv2
import numpy as np

from .image_utils import rotate_image, largest_rotated_rect, crop_around_center, rotate_point, resize_image


@dataclass
class FoundCrop:
    corners: np.ndarray
    rotation: float
    score: float


def find_crop(uncropped_image: np.ndarray, cropped_image: np.ndarray,
              resize_factor: float = 1.0, rotations=None,
              check_inverted: bool = True) -> FoundCrop:
    big   = resize_image(uncropped_image, factor=resize_factor)
    small = resize_image(cropped_image,   factor=resize_factor)

    bh, bw = big.shape[0:2]
    sh, sw = small.shape[0:2]

    default_corners = np.array(((0, 0), (bw, 0), (bw, bh), (0, bh)))
    best_match = FoundCrop(corners=default_corners, rotation=0, score=0)

    if rotations is None:
        rots = np.linspace(-1, 1, 51)
        rotations = 4 * rots ** 3 + rots

    for rot in rotations:
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
            continue

        if check_inverted:
            try:
                res2 = cv2.matchTemplate(big_rotated_cropped, cv2.bitwise_not(small), cv2.TM_CCOEFF_NORMED)
                if res2.max() > res.max():
                    res = res2
            except cv2.error:
                pass

        rm = res.max()
        if rm > best_match.score:
            loc = np.where(res == rm)
            for pt in zip(*loc[::-1]):
                x0, y0 = pt[0], pt[1]
                x1, y1 = x0 + sw, y0
                x2, y2 = x0 + sw, y0 + sh
                x3, y3 = x0,      y0 + sh

                ox, oy = cw / 2, ch / 2
                x0, y0 = rotate_point(x0, y0, ox, oy, rot)
                x1, y1 = rotate_point(x1, y1, ox, oy, rot)
                x2, y2 = rotate_point(x2, y2, ox, oy, rot)
                x3, y3 = rotate_point(x3, y3, ox, oy, rot)

                dx, dy = (bw - cw) / 2, (bh - ch) / 2
                points = np.array((
                    (x0 + dx, y0 + dy),
                    (x1 + dx, y1 + dy),
                    (x2 + dx, y2 + dy),
                    (x3 + dx, y3 + dy),
                ))
                if np.any(points) < 0:
                    continue
                if np.any(points[:, 0] > bw) or np.any(points[:, 1] > bh):
                    continue

                best_match = FoundCrop(corners=points, rotation=rot, score=rm)
                break

    best_match.corners = (best_match.corners / resize_factor).astype(int)
    return best_match
