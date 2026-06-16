import math

import numpy as np


def transform_corners_to_deskewed(
    corners: np.ndarray,
    rotation: float,
    img_w: int,
    img_h: int,
) -> tuple[int, int, int, int]:
    """
    Rotate corner points from original-image space into deskewed space and
    return the axis-aligned bounding box as (top, left, bottom, right) pixels.
    """
    ox, oy = img_w / 2.0, img_h / 2.0
    angle_rad = -math.radians(rotation)
    cos_a, sin_a = math.cos(angle_rad), math.sin(angle_rad)

    xs, ys = [], []
    for x, y in corners:
        dx, dy = float(x) - ox, float(y) - oy
        xs.append(cos_a * dx - sin_a * dy + ox)
        ys.append(sin_a * dx + cos_a * dy + oy)

    return int(min(ys)), int(min(xs)), int(max(ys)), int(max(xs))


def build_xmp(crop_angle: float, top: int, left: int, bottom: int, right: int) -> str:
    """Return an XMP sidecar string using the Adobe Camera Raw Settings namespace."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<x:xmpmeta xmlns:x="adobe:ns:meta/">\n'
        '  <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">\n'
        '    <rdf:Description rdf:about=""\n'
        '      xmlns:crs="http://ns.adobe.com/camera-raw-settings/1.0/"\n'
        f'      crs:HasCrop="True"\n'
        f'      crs:CropAngle="{crop_angle}"\n'
        f'      crs:CropTop="{top}"\n'
        f'      crs:CropLeft="{left}"\n'
        f'      crs:CropBottom="{bottom}"\n'
        f'      crs:CropRight="{right}"\n'
        f'      crs:CropUnit="3"/>\n'
        '  </rdf:RDF>\n'
        '</x:xmpmeta>'
    )
