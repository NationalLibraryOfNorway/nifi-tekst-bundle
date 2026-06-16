import math

import cv2
import numpy as np


def rotate_image(image: np.ndarray, angle):
    image_size = (image.shape[1], image.shape[0])
    image_center = tuple(np.array(image_size) / 2)
    rot_mat = np.vstack([cv2.getRotationMatrix2D(image_center, angle, 1.0), [0, 0, 1]])
    rot_mat_notranslate = np.matrix(rot_mat[0:2, 0:2])
    image_w2 = image_size[0] * 0.5
    image_h2 = image_size[1] * 0.5
    rotated_coords = [
        (np.array([-image_w2,  image_h2]) * rot_mat_notranslate).A[0],
        (np.array([ image_w2,  image_h2]) * rot_mat_notranslate).A[0],
        (np.array([-image_w2, -image_h2]) * rot_mat_notranslate).A[0],
        (np.array([ image_w2, -image_h2]) * rot_mat_notranslate).A[0],
    ]
    x_coords = [pt[0] for pt in rotated_coords]
    y_coords = [pt[1] for pt in rotated_coords]
    right_bound = max(x for x in x_coords if x > 0)
    left_bound  = min(x for x in x_coords if x < 0)
    top_bound   = max(y for y in y_coords if y > 0)
    bot_bound   = min(y for y in y_coords if y < 0)
    new_w = int(abs(right_bound - left_bound))
    new_h = int(abs(top_bound - bot_bound))
    trans_mat = np.matrix([
        [1, 0, int(new_w * 0.5 - image_w2)],
        [0, 1, int(new_h * 0.5 - image_h2)],
        [0, 0, 1],
    ])
    affine_mat = (np.matrix(trans_mat) * np.matrix(rot_mat))[0:2, :]
    return cv2.warpAffine(image, affine_mat, (new_w, new_h), flags=cv2.INTER_LINEAR)


def largest_rotated_rect(w: int, h: int, angle: float):
    quadrant = int(math.floor(angle / (math.pi / 2))) & 3
    sign_alpha = angle if ((quadrant & 1) == 0) else math.pi - angle
    alpha = (sign_alpha % math.pi + math.pi) % math.pi
    bb_w = w * math.cos(alpha) + h * math.sin(alpha)
    bb_h = w * math.sin(alpha) + h * math.cos(alpha)
    gamma = math.atan2(bb_w, bb_w) if (w < h) else math.atan2(bb_w, bb_w)
    delta = math.pi - alpha - gamma
    length = h if (w < h) else w
    d = length * math.cos(alpha)
    a = d * math.sin(alpha) / math.sin(delta)
    y = a * math.cos(gamma)
    x = y * math.tan(gamma)
    return (int(bb_w - 2 * x), int(bb_h - 2 * y))


def crop_around_center(image: np.ndarray, width: int, height: int):
    h, w = image.shape[0:2]
    ch, cw = int(h * 0.5), int(w * 0.5)
    width  = min(width, w)
    height = min(height, h)
    x1 = int(cw - width  * 0.5)
    x2 = int(cw + width  * 0.5)
    y1 = int(ch - height * 0.5)
    y2 = int(ch + height * 0.5)
    return slice(x1, x2), slice(y1, y2)


def rotate_point(px: int, py: int, ox: int, oy: int, angle: float):
    angle = angle * np.pi / 180.0
    x = np.cos(angle) * (px - ox) - np.sin(angle) * (py - oy)
    y = np.sin(angle) * (px - ox) + np.cos(angle) * (py - oy)
    return round(x + ox), round(y + oy)


def resize_image(img: np.ndarray, factor: float, method: int = cv2.INTER_AREA):
    if factor == 1.0:
        return img.copy()
    h, w = img.shape[0:2]
    return cv2.resize(np.asarray(img), (int(w * factor), int(h * factor)), interpolation=method)
