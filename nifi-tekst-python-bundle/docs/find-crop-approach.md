# FindCrop: Algorithm and Design

## Problem

A physical newspaper or book page is scanned at full resolution (uncropped). An autocrop tool then produces a smaller image containing just the content area of that page (cropped). The NiFi processor must determine **where** in the uncropped image the crop came from — expressed as a deskew angle and an axis-aligned bounding box — so that the relationship between the two can be recorded in an XMP sidecar file.

The crop may be rotated relative to the uncropped image because the autocrop tool both deskews and trims the scan.

---

## Algorithm Overview

```
uncropped image ──┐
                  ├─► find_crop()
cropped image  ───┘       │
                          ├─(use_hough=True)──► _estimate_skew_hough()
                          │                          │ confident?
                          │              yes ◄────── ┤
                          │               │          │ no
                          │    seeded ±2° search     │
                          │               │ score≥0.5?
                          │    return ◄───┘      │ no
                          │                      │
                          └──────────────────────┘
                          │
                          ▼  full ±5° cubic grid search
                     FoundCrop (corners, rotation, score, used_hough)
                          │
                          ▼
               transform_corners_to_deskewed()
                          │
                          ▼
               snap_bounds_to_known_size()
                          │
                          ▼
               build_xmp() ──► XMP sidecar
```

---

## Step 1: Resize for Speed

Both images are downsampled by `resize_factor` (default 0.1) before any matching.

`cv2.matchTemplate` cost is O((W−w) × (H−h) × w × h) per rotation angle. At full resolution on a 6200×7900 uncropped image with a 5000×7000 template this would take minutes. At factor 0.1 it takes under a second.

Crucially, reducing `resize_factor` below 0.1 does not change position accuracy for these images because the template (crop) covers nearly the entire uncropped image, leaving very little search space regardless of resolution. The resize is a speed mechanism, not an accuracy trade-off for this use case.

---

## Step 2: Hough-Seeded Fast Path

Before the full grid search, the algorithm optionally estimates the document skew using the Hough line transform (`use_hough=True` by default).

### Why Hough

Document pages have strong near-horizontal text baselines. If Hough can reliably detect those lines, their mean angle is a direct estimate of the skew — no rotation search needed. This seeds a narrow ±2° window around the estimate instead of scanning the full ±5° grid, which:

- Reduces the number of `warpAffine + matchTemplate` calls from ~71 to ~21 for the seeded pass
- Extends the effective rotation range: a confident Hough angle can seed the search even outside the normal ±5° grid

### How it works

1. Convert the uncropped image to greyscale and downsample to **0.2 scale** (fixed, independent of `resize_factor`; Hough needs more detail than template matching does).
2. Run Canny edge detection (thresholds 50/150).
3. Run `cv2.HoughLines` with 1 px resolution and **0.1° angular resolution**. The accumulator threshold is `max(10, width × 0.25)` — a line must be supported by at least 25% of the image width.
4. Keep only lines whose angle is within ±10° of horizontal (i.e., `|θ − 90°| < 10°`).
5. If fewer than `min_lines=10` survive, or if their standard deviation exceeds `max_std_deg=0.8°`, return `None` — not confident enough.
6. Otherwise return the **median** angle as the skew estimate.

### Fallback logic

The seeded search runs 21 angles from `[estimate − 2°, estimate + 2°]` with the same two-phase refinement as the full search. If the best score is ≥ `hough_fallback_threshold` (default 0.5), the result is returned immediately with `used_hough=True`.

If Hough returns `None`, or if the seeded score falls below the threshold, the full ±5° cubic grid search runs as normal and `used_hough` is `False`.

The FlowFile attribute `crop.used_hough` (`true`/`false`) records which path was taken.

---

## Step 3: Coarse Rotation Search

The algorithm searches across a range of candidate rotation angles. Since most document skew is small, the angles are distributed non-uniformly using a cubic mapping:

```
rots   = linspace(-1, 1, 51)       # 51 uniformly-spaced samples
angles = 4·rots³ + rots            # range ≈ [-5°, +5°], dense near 0°
```

The cubic `f(x) = 4x³ + x` has derivative `f'(x) = 12x² + 1`, so grid spacing in degrees is:

| Region | Step size | Positional error (7000px image) |
|--------|-----------|--------------------------------|
| Near 0° | ~0.04° | ±2.5 px |
| Near 1° | ~0.07° | ±4.4 px |
| Near 2° | ~0.16° | ±9.8 px |
| Near 3° | ~0.28° | ±17 px |
| Near 5° | ~0.50° | ±31 px |

For each candidate angle:

1. Rotate the (downsampled) uncropped image by that angle using `warpAffine` with bilinear interpolation — the canvas expands to avoid clipping corners.
2. Trim the expanded canvas to the largest axis-aligned rectangle that fits inside the rotated image (`largest_rotated_rect`), discarding the black border introduced by the warp.
3. Run `cv2.matchTemplate(rotated_uncropped, cropped, TM_CCOEFF_NORMED)` to find where the cropped image fits inside the rotated uncropped image. If `check_inverted=True`, also try matching the colour-inverted template and keep whichever score is higher (handles images where tones have been inverted during processing).
4. Record the best match position and score.

---

## Step 4: Two-Phase Rotation Refinement

The coarse grid leaves residual error of up to 0.5° at large angles. Two techniques reduce this:

### Parabolic interpolation (free)

Given the best coarse angle `r₁` and scores at its neighbours `r₀`, `r₂`:

```
peak = r₁ − ½ · (r₂ − r₀) · (s₂ − s₀) / (s₀ − 2s₁ + s₂)
```

This fits a parabola through the three points and returns the analytical maximum. The result is clamped to `[r₀, r₂]` to prevent extrapolation.

### Fine grid search

A second pass searches `refine_steps=20` angles spread evenly over `[peak − step, peak + step]`, where `step` is the coarse grid spacing at the winning angle. This concentrates template matches exactly where the score is highest.

Combined effect measured on synthetic images:

| True angle | Coarse error | Refined error | Improvement |
|-----------|-------------|--------------|-------------|
| 0.3° | 0.068° | 0.043° | 1.6× |
| 1.0° | 0.082° | 0.038° | 2.2× |
| 2.0° | 0.062° | 0.018° | 3.4× |
| 3.5° | 0.106° | 0.012° | 8.5× |

The improvement is largest at larger angles because that is where the coarse grid is sparsest. At 3.5° the refined error of 0.012° causes less than 1.5 px of top/bottom misalignment on a 7000 px image.

---

## Step 5: Back-Project Corners into Original Image Space

The template match finds the top-left position of the crop in the **rotated** uncropped image. The corresponding four corners of the crop rectangle must be projected back into the **original** (unrotated) uncropped image space.

For each corner `(x, y)` in the rotated-and-trimmed space:

1. Add the trim offset `(dx, dy) = ((bw − cw)/2, (bh − ch)/2)` to move from trimmed space to the full rotated canvas.
2. Apply `rotate_point(x, y, cx, cy, −rot)` — rotating by the **negative** of the search angle around the canvas centre `(cx, cy)`. The minus sign is essential: `rotate_image` rotated the uncropped by `+rot`, so reversing that requires `−rot`.
3. The result is the corner position in the original uncropped image at downsampled resolution.

Corners that land outside the image bounds are discarded.

---

## Step 6: Scale Back to Full Resolution

All corner coordinates are divided by `resize_factor` to recover full-resolution pixel positions:

```python
best_match.corners = (best_match.corners / resize_factor).astype(int)
```

---

## Step 7: Transform Corners to Deskewed Bounds

The corners are in the original (skewed) image space. To get axis-aligned crop bounds in the **deskewed** space — which is what the XMP needs — each corner is rotated by `−rotation` around the image centre:

```python
angle_rad = −radians(rotation)
x' = cos(a)·(x − cx) − sin(a)·(y − cy) + cx
y' = sin(a)·(x − cx) + cos(a)·(y − cy) + cy
```

The bounding box of the four transformed corners gives `(top, left, bottom, right)`.

---

## Step 8: Snap Bounds to Known Size

The `/ resize_factor` step in Step 5 introduces a systematic dimension error:

```
floor(W × factor) / factor ≠ W
```

At factor 0.1 this caused ~84 px width error and ~58 px height error on a 5000×7000 crop. Since the actual dimensions of the cropped image are known exactly, the fix is to keep the found centre and force the box to the correct size:

```python
cx = (left + right) / 2
cy = (top  + bottom) / 2
left   = round(cx − exact_w / 2)
top    = round(cy − exact_h / 2)
right  = left + exact_w
bottom = top  + exact_h
```

After snapping, crop dimensions are exact regardless of `resize_factor`.

---

## Step 9: XMP Output

The final bounds and rotation angle are written into an XMP sidecar using the Adobe Camera Raw Settings (`crs:`) namespace:

```xml
<rdf:Description rdf:about=""
  xmlns:crs="http://ns.adobe.com/camera-raw-settings/1.0/"
  crs:HasCrop="True"
  crs:CropAngle="{rotation}"
  crs:CropTop="{top}"
  crs:CropLeft="{left}"
  crs:CropBottom="{bottom}"
  crs:CropRight="{right}"
  crs:CropUnit="3"/>
```

`CropUnit=3` means pixel coordinates. `CropAngle` is the deskew rotation in degrees. The bounds describe the crop region in the deskewed uncropped image.

---

## Optional: ALTO Dimension Validation

When an ALTO XML path is provided, the processor validates that the found crop dimensions are consistent with the ALTO page dimensions. NB access images are scanned at 400 DPI, giving a fixed scale of `400 / (25.4 × 10) ≈ 1.5748 px/mm10`.

```
expected_w = round(alto_width_mm10  × 1.5748)
expected_h = round(alto_height_mm10 × 1.5748)
error_w    = |found_w − expected_w|
error_h    = |found_h − expected_h|
```

If either error exceeds the configured tolerance (default 200 px), the FlowFile is routed to the `dimension_mismatch` relationship. This catches wrong-ALTO situations (e.g., a primary-representation ALTO supplied instead of an access-representation ALTO) without relying on the cropped image dimensions for comparison.

---

## Parameters

| Parameter | Default | Effect |
|-----------|---------|--------|
| `resize_factor` | 0.1 | Downsample ratio before matching. Lower = faster, same accuracy for large crops. |
| `check_inverted` | true | Also try colour-inverted template; keeps the better score. |
| `use_hough` | true | Enable Hough-seeded fast path. Disable to always run the full grid (useful for benchmarking or images with no clear text lines). |
| `hough_fallback_threshold` | 0.5 | Minimum match score required to accept the Hough-seeded result. If the score falls below this, the full grid runs instead. |
| `refine_rotation` | true | Enable two-phase refinement. Disable only for benchmarking. |
| `refine_steps` | 20 | Number of fine-grid angles per refinement pass. |
| `Dimension Tolerance` | 200 px | Allowed ALTO vs found-crop size discrepancy. |

---

## Known Limitations

- **Rotation range**: ±5° for the full grid. When Hough succeeds, the seeded ±2° window is centred on the Hough estimate, so the effective range extends as far as Hough can reliably detect lines. Documents with no clear text lines and skew > 5° will produce a poor match score; the `rotations` parameter can be overridden to extend the grid range manually.
- **Template must fit inside uncropped**: the algorithm assumes the crop is a sub-region of the uncropped image. Padding or canvas extension in the original is not handled.
- **Pixel-perfect reconstruction not guaranteed**: applying the XMP crop to the uncropped image will give the same visual content but not identical pixel values, because bilinear rotation interpolation is not invertible and JP2 encoding introduces its own lossy rounding.
- **Fixed 400 DPI scale**: the ALTO validation assumes NB's access scan standard. Collections scanned at different DPI would need `NB_ACCESS_SCALE` adjusted in `alto_utils.py`.
